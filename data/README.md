This folder holds collected data and the scripts used to collect them.

In order to collect continuously, various scripts are set to run via a
cronjob. Run `generate_crontab.py` to generate set of possible crontab
entries and then paste them into the crontab editor, opening it via:

    $ crontab -e

