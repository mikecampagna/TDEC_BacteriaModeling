### TDEC_BacteriaModeling
Adapting the Watershed Treatment Model (WTM) from spreadsheet format to Python for implementation in an online tool. The study region is the Delaware Bay portion of the state of Delaware.

The base loads were calculated in SQL and stored in a PostgreSQL (v9.5) database. Baseline loads were calculated at three geographic scales: NHDplus catchments, WBD HUC12s, and DNREC Watersheds used for TMDLs. All related code is in the Code/SQL Folder. To rebuild this database, obtain the "bacteria_modeling.backup" file and restore with this command:

```
psql -U username -d bacteria_modeling < bacteria_modeling.backup
or
pg_restore -U username -d bacteria_modeling -1 bacteria_modeling.backup
```
The most importand tables from this database are as follows:
1. dnrec.nhdplus_tdec_bacterialoading
2. dnrec.huc12_tdec_bacterialoading
3. dnrec.dnrecws_tdec_bacterialoading

[Link to download a database backup.](https://drexel0-my.sharepoint.com/:f:/g/personal/msc94_drexel_edu/EkZvrgBDHAROvyM7jZGIcB0BufKoV72L5uD2SzxqxE456A?e=T5w61V)

A Python program was then created to grab the base loads from the database, and run the bacteria load reductions (in the Python folder). This requires only a basic Python environment and a simple config file to connect to the database of which there is an example of in the Python folder. It is always recommended to add the production config file to the .gitignore. To test the Python function, edit the inputs in the "load_bacteria_model.py" and then run it from a command prompt.

```
conda env create --name tdec --file=basic_conda_env.yml

python load_bacteria_model.py
```
