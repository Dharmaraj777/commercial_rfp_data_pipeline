    @staticmethod
    def _key_from_hash(text: str, algo: str = "md5") -> str:
        """Build a stable RFP_Content_* hash from the FULL input text."""
        if text is None:
            text = ""
        data = text.encode("utf-8")  # ✅ no extra truncation here

        if algo == "md5":
            hash_hex = hashlib.md5(data).hexdigest()
        elif algo == "sha1":
            hash_hex = hashlib.sha1(data).hexdigest()
        elif algo == "sha256":
            hash_hex = hashlib.sha256(data).hexdigest()
        else:
            raise ValueError(f"Unsupported hash algorithm: {algo}")

        return f"RFP_Content_{hash_hex}"



    def _add_rfp_keys(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        Add:
          - key: human readable, uses first 120 chars of Q & A
          - key_hash: stable hash, uses FULL question + FULL response (+ metadata)
        """
        df = df.copy()

        # Normalize date to a consistent string form if present
        if "date" in df.columns:
            df["date"] = pd.to_datetime(df["date"], errors="coerce").dt.strftime("%Y-%m-%d")

        def build_key_parts(row):
            client = str(row.get("client name", "")).strip()
            rfp_type = str(row.get("rfp type", "")).strip()
            consultant = str(row.get("consultant", "")).strip()
            date = str(row.get("date", "")).strip()
            question = str(row.get("question", "")).strip()
            response = str(row.get("response", "")).strip()

            # Readable key (snippet)
            q_snip = question[:120]
            r_snip = response[:120]
            readable_key = f"{client}_{date}_{rfp_type}_{consultant}_{q_snip}_{r_snip}"

            # Hash input (FULL content, no clipping)
            hash_input = f"{client}|{date}|{rfp_type}|{consultant}|{question}|{response}"

            return readable_key, hash_input

        # Build both values in one pass
        keys = df.apply(build_key_parts, axis=1, result_type="expand")
        keys.columns = ["key", "_hash_input"]

        df["key"] = keys["key"]

        # 👉 IMPORTANT: we don't strip all whitespace now; we keep it as part of the hash
        df["key_hash"] = keys["_hash_input"].apply(lambda x: self._key_from_hash(x, algo="md5"))

        return df
