import pandas as pd
import hashlib


def key_from_hash(question, algo):
    snippet = question[:120]  # or use the full question, depending on preference
    data = snippet.encode('utf-8')
    if algo == 'md5':
        hash_hex = hashlib.md5(data).hexdigest()
    elif algo == 'sha1':
        hash_hex = hashlib.sha1(data).hexdigest()
    elif algo == 'sha256':
        hash_hex = hashlib.sha256(data).hexdigest()
    else:
        raise ValueError("Unsupported hash algorithm")
    return 'RFP_Content_'+hash_hex


def generate_rfp_key(df):
    df['Date'] = pd.to_datetime(df['Date'], errors='coerce').dt.strftime('%Y-%m-%d')
    df['key'] = df.apply(lambda row: f"{str(row['Client Name']).strip()}_{str(row['Date']).strip()}_{str(row['RFP Type']).strip()}_{str(row['Question']).strip()}", axis=1)
    df['key_hash'] = df['key'].str.replace(' ','').apply(lambda x: key_from_hash(x, algo='md5'))
    return df



filepath = r"C:\Users\ODKPTSO\Downloads\RFP Content June 2025.xlsx"
df = pd.read_excel(filepath)

df=df[['Client Name',	'RFP Type',	'Consultant',	'Date',	'Question',	'Response']]
df = generate_rfp_key(df)

df.head(2)

======================================================

    def reset_indexer(self, indexer_client: SearchIndexerClient, indexer_name: str) -> None:
        """
        Reset the indexer and then re-run it after a short delay.
        """
        try:
            logger.info(f"Resetting indexer: {indexer_name}")
            indexer_client.reset_indexer(indexer_name)

            # Delay before re-running
            logger.info("Waiting 10 seconds before running indexer...")
            time.sleep(10)

            logger.info(f"Running indexer: {indexer_name}")
            indexer_client.run_indexer(indexer_name)

            logger.info(f"Indexer '{indexer_name}' has been reset and re-run successfully.")
        except Exception as e:
            logger.exception(f"Error while resetting and running indexer '{indexer_name}': {e}")

