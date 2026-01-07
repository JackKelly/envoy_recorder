## Envoy Recorder

Capture solar PV data from the Enphase Envoy over the LAN, save locally to disk, and upload.

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
