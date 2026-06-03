import re
import numpy as np
import pandas as pd
from hashlib import md5


NAMESPACE_MAP = {
    0: "article", 1: "talk", 2: "user", 3: "user_talk",
    4: "wikipedia", 5: "wikipedia_talk", 6: "file", 7: "file_talk",
    8: "mediawiki", 9: "mediawiki_talk", 10: "template", 11: "template_talk",
    12: "help", 13: "help_talk", 14: "category", 15: "category_talk",
    100: "portal", 101: "portal_talk", 108: "book", 109: "book_talk",
    118: "draft", 119: "draft_talk", 446: "education_program",
    710: "timedtext", 828: "module", 829: "module_talk",
}
ALL_NAMESPACES = sorted(set(NAMESPACE_MAP.values())) + ["other"]
TEXT_FIELDS = ["wiki", "server_name", "comment", "parsedcomment"]
HASH_BUCKETS = 64
FORBIDDEN = {"bot", "user", "id", "title_url", "server_url", "server_script_path"}


class WikipediaFeatureEngineer:

    def __init__(self):
        self._feature_names = None

    def fit_transform_from_file(self, path: str) -> tuple:
        df = pd.read_parquet(path) if path.endswith(".parquet") else pd.read_csv(path, low_memory=False)
        if "bot" not in df.columns:
            raise ValueError("Brak kolumny 'bot', nie można  targetu.")
        y = df["bot"].astype(int)
        return self.transform_df(df), y

    def transform_record(self, raw: dict) -> pd.DataFrame:
        if "length_old" not in raw:
            length, revision = raw.get("length") or {}, raw.get("revision") or {}
            raw = {**raw, "length_old": length.get("old"), "length_new": length.get("new"),
                         "revision_old": revision.get("old"), "revision_new": revision.get("new")}
        return self.transform_df(pd.DataFrame([raw]))

    def transform_df(self, df: pd.DataFrame) -> pd.DataFrame:
        df = df.drop(columns=[c for c in FORBIDDEN if c in df.columns])
        result = pd.concat([
            self._parse_numeric_fields(df),
            self._compute_edit_features(df),
            self._encode_namespace(df),
            self._hash_text_fields(df),
        ], axis=1)

        if self._feature_names is None:
            self._feature_names = list(result.columns)
        return result.reindex(columns=self._feature_names, fill_value=0)

    def get_feature_names(self) -> list:
        if self._feature_names is None:
            raise RuntimeError("Wywołaj najpierw transform_df() lub fit_transform_from_file().")
        return self._feature_names

    @staticmethod
    def _col(df, name, fill=np.nan):
        return pd.to_numeric(df[name], errors="coerce") if name in df.columns else pd.Series(fill, index=df.index)

    def _parse_numeric_fields(self, df: pd.DataFrame) -> pd.DataFrame:
        result = pd.DataFrame(index=df.index)
        for col in ["length_old", "length_new", "revision_old", "revision_new"]:
            result[col] = self._col(df, col)
        return result

    def _compute_edit_features(self, df: pd.DataFrame) -> pd.DataFrame:
        r  = pd.DataFrame(index=df.index)
        len_old = self._col(df, "length_old")
        len_new = self._col(df, "length_new")
        rev_old = self._col(df, "revision_old")
        comment = self._col(df, "comment", fill="").fillna("").astype(str)
        title = self._col(df, "title",   fill="").fillna("").astype(str)

        r["has_length"] = len_old.notna().astype(int)
        r["has_revision"] = rev_old.notna().astype(int)
        r["length_diff"] = len_new - len_old
        r["length_diff_abs"] = r["length_diff"].abs()
        r["length_ratio"] = np.where(len_old.notna() & (len_old > 0), len_new / len_old, np.nan)
        r["is_new_page"] = rev_old.isna().astype(int)
        r["is_minor"] = self._col(df, "minor",     fill=0).fillna(0).astype(int)
        r["is_patrolled"] = self._col(df, "patrolled", fill=0).fillna(0).astype(int)
        r["comment_length"] = comment.str.len()
        r["has_comment"] = (comment.str.strip() != "").astype(int)
        r["title_has_number"] = title.str.contains(r"\d", regex=True).astype(int)
        return r

    @staticmethod
    def _encode_namespace(df: pd.DataFrame) -> pd.DataFrame:
        result = pd.DataFrame(0, index=df.index, columns=[f"ns_{ns}" for ns in ALL_NAMESPACES])
        if "namespace" not in df.columns:
            return result
        labels = df["namespace"].map(lambda x: NAMESPACE_MAP.get(int(x) if pd.notna(x) else -1, "other"))
        for ns in ALL_NAMESPACES:
            result[f"ns_{ns}"] = (labels == ns).astype(int)
        return result

    def _hash_text_fields(self, df: pd.DataFrame) -> pd.DataFrame:
        frames = []
        for field in TEXT_FIELDS:
            matrix = np.zeros((len(df), HASH_BUCKETS), dtype=np.float32)
            values = (df[field] if field in df.columns else pd.Series("", index=df.index)).fillna("").astype(str)
            for i, text in enumerate(values):
                for token in re.findall(r"[a-z0-9]+", text.lower()):
                    matrix[i, int(md5(token.encode(), usedforsecurity=False).hexdigest(), 16) % HASH_BUCKETS] += 1
            frames.append(pd.DataFrame(matrix, columns=[f"hash_{field}_{j}" for j in range(HASH_BUCKETS)], index=df.index))
        return pd.concat(frames, axis=1)


# --- Demo ---
if __name__ == "__main__":
    edit = {"id": 1, "type": "edit", "namespace": 0, "title": "Python", "comment": "fixed typo",
            "parsedcomment": "fixed typo", "timestamp": 1700000000, "user": "Ed", "bot": False,
            "minor": True, "patrolled": False, "server_name": "en.wikipedia.org", "wiki": "enwiki",
            "length": {"old": 12000, "new": 12010}, "revision": {"old": 1000, "new": 1001}}

    log_ev = {"id": 2, "type": "log", "namespace": 2, "title": "User:Bot", "comment": "account created",
              "timestamp": 1700000100, "user": "Bot", "bot": True,
              "server_name": "en.wikipedia.org", "wiki": "enwiki"}  # brak: length, revision, minor, patrolled

    fe = WikipediaFeatureEngineer()
    X_edit = fe.transform_record(edit)
    X_log = fe.transform_record(log_ev)

    print(f"Liczba cech: {X_edit.shape[1]}")
    print(f"Kolumny identyczne: {list(X_edit.columns) == list(X_log.columns)}")
    print("\nhas_length / has_revision:")
    print(f"  edit → {X_edit[['has_length','has_revision']].values}")
    print(f"  log  → {X_log[['has_length','has_revision']].values}")