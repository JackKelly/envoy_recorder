## Envoy Recorder

Capture solar PV data from the Enphase Envoy over the LAN, save locally to disk, and upload to
Hugging Face.

Note that this code has only been tested with Enphase IQ7+ micro-inverters.

This script does the following. Note that this script is designed to be called from cron once per
minute.

1. The script gets data from the Enphase Envoy over the LAN and saves the Envoy's text response to a
   file in the path `config.paths.live_buffer_incoming`. The filename is just the unix timestamp (in
seconds), with the suffix `json`. This stage is deliberately very simple. The script doesn't even
try to parse the JSON. Nor does it de-duplicate the data (and a lot of this data will be duplicates
because the Envoy only updates the report for each microinverter once every 15 minutes). We poll
once per minute to give lots of chances to get the data, because the Envoy occasionally stops
responding to requests (perhaps because it's uploading to Enphase). This stage of the script just
saves the Envoy's response to disk. This should mean that data will be saved even if the Envoy
returns invalid JSON, or if Enphase changes the JSON schema. We use this approach of saving live
data into a "write ahead log" (instead of saving directly to Parquet every minute) because the only
way to append to Parquet is to _replace_ the entirety of the most recent monthly partition. And it
would be unkind to the SSD's flash memory to re-write that Parquet file every minute! Also, by
writing the Envoy's HTTP response to disk as a text file, we can more easily debug JSON schema
changes.
2. Every `config.intervals.flush_buffer_every_n_minutes`, the script will rename
   `live_buffer/incoming` to `live_buffer/processing_<timestamp>`, and load the JSON files into a
Polars DataFrame, and then write that DataFrame to `config.paths.parquet_archive` in Hive
partitioned format, and delete `processing_<timestamp>`.

## Setup

1. Get an API token to allow you to access your Envoy. 
   Go to https://entrez.enphaseenergy.com/entrez_tokens and start typing your PV system name in the 
   "Select System" box. And then select your gateway. (You can find your PV system name in the 
   Enphase smartphone app. Go to menu (bottom right), and your system name should be displayed 
   near the top of the screen. For example, my system name is "Kelly").
2. Pull this git repo.
3. Create a `config.toml` file. See `src/envoy_recorder/config_loader.py` for details of what needs
   to go into `config.toml`.
4. Test by running `uv run scripts/record.py`
5. Configure `cron` to run `scripts/record.py` regularly. I run it once per minute with the
   following `crontab` job: `* * * * * cd /home/jack/dev/python/envoy_recorder && /snap/bin/uv run
scripts/record.py >> /home/jack/dev/python/envoy_recorder/logs/cron_record.log 2>&1`


