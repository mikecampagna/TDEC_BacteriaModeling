create schema raster;

E:\SPATIAL\UVM
de1m_lu_18n.tif
de1m_lc_18n.tif

raster2pgsql -s 32618 -d -I -t 250x250 -M E:\SPATIAL\UVM\de1m_lu_18n.tif -F  raster.de1m_lu_18n > E:\SPATIAL\UVM\de1m_lu_18n.sql
raster2pgsql -s 32618 -d -I -t 250x250 -M E:\SPATIAL\UVM\de1m_lc_18n.tif -F  raster.de1m_lc_18n > E:\SPATIAL\UVM\de1m_lc_18n.sql

mput *.sql

psql -U drwiadmin -h localhost -p 5432 -d drwi -f de1m_lu_18n.sql 
psql -U drwiadmin -h localhost -p 5432 -d drwi -f de1m_lc_18n.sql 



