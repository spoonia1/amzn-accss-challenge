import os
import dropbox


# set the working directory to download the file
# os.chdir('/home.../siddharth/uber_automation/input')
path = os.getcwd()

# connect to dropbox api
dbx = dropbox.Dropbox('_11NObBtNPQAAAAAAAAAAYYeyI4gY-i6k8_foxTpaLLrnUcl_BBzogjqgh3NizOh')

# get latest file name 
to_get = dbx.files_list_folder('/daily/redseer-international-2020/india-uber/uber-report').entries[-1].name
d_path = '/daily/redseer-international-2020/india-uber/uber-report'
# download the file
dbx.files_download_to_file(path + '/' + to_get ,d_path + '/' + to_get)

# dbx.files_download_to_file('..uber_automation/daily' + '/' + to_get ,d_path + '/' + to_get)
# delete top file 
to_del = dbx.files_list_folder('/daily/redseer-international-2020/india-uber/uber-report').entries[0].name
# dbx.files_delete(d_path + '/' + to_del)