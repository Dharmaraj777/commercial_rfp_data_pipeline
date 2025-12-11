    def _log_duplicate_keys(self, df: pd.DataFrame, context: str) -> None:
        """
        Log and report any duplicate key_hash values in the given dataframe.
        `context` is just a label ('raw' / 'final') for the logs.
        """
        if "key_hash" not in df.columns:
            logger.warning(f"[{context}] key_hash column not present, cannot check duplicates.")
            return

        dup_mask = df["key_hash"].duplicated(keep=False)
        if not dup_mask.any():
            logger.info(f"[{context}] No duplicate key_hash values detected.")
            return

        dup_df = df.loc[dup_mask].copy()
        n_dup_keys = dup_df["key_hash"].nunique()
        n_rows = dup_df.shape[0]

        logger.warning(
            f"[{context}] Detected {n_dup_keys} duplicate key_hash values "
            f"across {n_rows} rows."
        )

        # Log a few examples to the log file
        sample = dup_df[["key_hash", "client name", "rfp type", "consultant", "date", "question", "response"]].head(10)
        logger.warning(f"[{context}] Sample duplicate key rows:\n{sample.to_string(index=False)}")


===============================================
