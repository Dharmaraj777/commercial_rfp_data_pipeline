    def _add_rfp_keys(self, df: pd.DataFrame) -> pd.DataFrame:
        """Add `key` and `key_hash` columns to the final cleaned RFP dataframe."""
        df = df.copy()

        # Normalize date to a consistent string form if present
        if "date" in df.columns:
            df["date"] = pd.to_datetime(df["date"], errors="coerce").dt.strftime("%Y-%m-%d")

        def build_key(row):
            client = str(row.get("client name", "")).strip()
            rfp_type = str(row.get("rfp type", "")).strip()
            consultant = str(row.get("consultant", "")).strip()
            date = str(row.get("date", "")).strip()
            question = str(row.get("question", "")).strip()
            response = str(row.get("response", "")).strip()

            q_snip = question[:120]
            r_snip = response[:120]

            return f"{client}_{date}_{rfp_type}_{consultant}_{q_snip}_{r_snip}"

        df["key"] = df.apply(build_key, axis=1)
        df["key_hash"] = (
            df["key"]
            .str.replace(r"\s+", "", regex=True)
            .apply(lambda x: self._key_from_hash(x, algo="md5"))
        )
        return df


==========================================

            items_after = self._list_sharepoint_items(
                site_id, drive_id, relative_folder_path, access_token
            )
            logger.info(f"Found {len(items_after)} files in SharePoint citation folder after cleanup.")

            mapping_rows = []
            for item in items_after:
                name = item.get("name")
                url = item.get("webUrl")
                if not name or not name.lower().endswith(".docx"):
                    continue
                if url:
                    mapping_rows.append(
                        {
                            "file_name": name,
                            "preview_url": url,
                        }
                    )

            # 6) Build mapping DataFrame
            if mapping_rows:
                df = pd.DataFrame(mapping_rows)
            else:
                df = pd.DataFrame(columns=["file_name", "preview_url"])

            # ---- DUPLICATE KEY CHECK (mapping-level) ----
            if not df.empty:
                dup_mask = df["file_name"].duplicated(keep=False)
                if dup_mask.any():
                    dup_df = df.loc[dup_mask].sort_values("file_name")
                    dup_names = dup_df["file_name"].unique().tolist()
                    logger.warning(
                        f"[MAPPING] Detected duplicate file_name keys in mapping: "
                        f"{len(dup_names)} duplicated keys. Examples: {dup_names[:10]}"
                    )
                    # If you want to be VERY strict, you could raise instead:
                    # raise ValueError(f"Duplicate file_name keys detected in mapping: {dup_names}")
                # Ensure mapping we write has unique keys
                df = df.drop_duplicates(subset=["file_name"], keep="last")

            mapping_blob_name = self.mapping_filename or "rfp_content_docx_preview_mapping.xlsx"

            self.utils.upload_result_to_blob_container(
                mapping_blob_name,
                df,
                self.output_container_name,
                self.blob_service_client
            )

            logger.info(
                f"Citation mapping written to blob '{mapping_blob_name}' "
                f"with {len(df)} unique keys (file_name)."
            )
