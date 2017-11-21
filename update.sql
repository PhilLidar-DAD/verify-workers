start transaction;
update result set file_path = replace(file_path, '\\', '/');
commit;
