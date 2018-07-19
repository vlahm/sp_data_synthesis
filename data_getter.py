import ftplib
import re
import libarchive.public
import os
import gc

def get_dir_contents():
    ls_out = []
    ftp.dir(ls_out.append)
    return ls_out

def list_only_filenames(search_string, dir_list):
    files = []
    for x in range(len(dir_list)):
        m = re.match('.*(?:\s)(.*' + search_string + '.*)$', dir_list[x])
        if m:
            files.append(m.group(1))
    return files

def enter_and_search(search_string, dir):
    ftp.cwd(dir)
    dir_info = get_dir_contents()
    file_names = list_only_filenames(search_string, dir_info)
    return file_names

def download_and_extract(search_string, file_list, zip_type):
    for x in range(len(file_list)):
        m = re.match('(.*(?:' + search_string + ')(?:.*)).' + zip_type,
            file_list[x])
        if m:
            filename_base = m.group(1)
            zipfile = wd + '/' + filename_base + '.' + zip_type

            try:
                with open(zipfile, 'wb') as fp:
                    res = ftp.retrbinary('RETR ' + filename_base + '.' + zip_type,
                        fp.write)
                if res.startswith('226'):
                    if zip_type == '7z':
                        os.system('7z x ' + zipfile + ' -o' + wd)
                        # os.remove(zipfile)
                        print('Downloaded and extracted: ' + filename_base)
                    elif zip_type == 'zip':
                        os.system('unzip ' + zipfile + ' -d ' + wd)
                        os.remove(zipfile)
                        print('Downloaded and extracted: ' + filename_base)
                    else:
                        print('zip type not yet handled: ' + filename_base)
                else:
                    print('Download failed: ' + filename_base)
                    if os.path.isfile(zipfile):
                        os.remove(zipfile)

            except ftplib.all_errors as e:
                print('FTP error:', e)
                if os.path.isfile(zipfile):
                    os.remove(zipfile)

            gc.collect()

#acquire NHDPlusV2 catchments for all regions
wd = '/home/mike/git/streampulse/jim_projects/nhd_catchments'

ftp = ftplib.FTP('ftp.horizon-systems.com')
ftp.login() #login anonymously
ftp.cwd('NHDplus/NHDPlusV21/data')

f = get_dir_contents()
#region_dirs = list_only_filenames('NHDPlus(?!C)', f)
region_dirs = list_only_filenames('NHDPlusMS', f)

for r in region_dirs:
    subfiles = enter_and_search('NHDPlus', r)
    if len(subfiles) < 8: #subfiles are actually subregion directories
        for f in subfiles:
            subfiles2 = enter_and_search('NHDPlus', f)
            download_and_extract('NHDPlusCatchment', subfiles2, '7z')
            ftp.cwd('..')
        ftp.cwd('..')
    else:
        download_and_extract('NHDPlusCatchment', subfiles, '7z')
        ftp.cwd('..')

ftp.quit()

#acquire streamcat data
wd = '/home/mike/git/streampulse/jim_projects/streamcat_data'

ftp = ftplib.FTP('newftp.epa.gov')
ftp.login() #login anonymously
ftp.cwd('EPADataCommons/ORD/NHDPlusLandscapeAttributes/StreamCat/HydroRegions')

f = get_dir_contents()
files = list_only_filenames('BFI_Region', f)
download_and_extract('BFI_Region', files, 'zip')
files = list_only_filenames('CanalDensity', f)
download_and_extract('CanalDensity', files, 'zip')
files = list_only_filenames('Dams_Region', f)
download_and_extract('Dams_Region', files, 'zip')
files = list_only_filenames('Runoff_Region', f)
download_and_extract('Runoff_Region', files, 'zip')
files = list_only_filenames('NLCD2011_Region', f)
download_and_extract('NLCD2011_Region', files, 'zip')
files = list_only_filenames('ForestLossByYear0013_Region', f)
download_and_extract('ForestLossByYear0013_Region', files, 'zip')
files = list_only_filenames('USCensus2010_Region', f)
download_and_extract('USCensus2010_Region', files, 'zip')
files = list_only_filenames('PRISM_1981_2010', f)
download_and_extract('PRISM_1981_2010', files, 'zip')

ftp.quit()


#other potentially useful stuff

# ftp.cwd(region_dirs[2])
# ftp.pwd()
# ftp.cwd('..')

# l = ftp.retrlines('LIST')
# n = ftp.retrlines('NLST')
# m = ftp.retrlines('MLSD')
# ftp.quit()

# wdir = ftp.sendcmd('PWD')
# print(ftplib.parse257(wdir))
