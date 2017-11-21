set autocommit=0;
update result set file_path = replace(file_path, '\\', '/') where file_path like '%\\\\%';
commit;
