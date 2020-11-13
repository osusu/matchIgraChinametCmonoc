import numpy as np
import pandas as pd


def readIgra2StationList(filename,save=False):
    """ Read IGRAv2 station list

    Args:
        filename (str): filename of station list
        verbose (inr): verboseness

    Returns:
        DataFrame : station informations
    """
    if '.txt' in filename:
        try:
            infile = open(filename)
            tmp = infile.read()
            data = tmp.splitlines()
            print("Igra2StationList Data read from: " + filename)
        except IOError as e:
            print("File not found: " + filename)
            raise e
        else:
            infile.close()

        out = pd.DataFrame(columns=['id_igra', 'wmo', 'lat', 'lon', 'alt', 'state', 'name', 'start', 'end', 'total'])

        for i, line in enumerate(data):
            id = line[0:11]

            try:
                id2 = "%05d" % int(line[6:11])  # substring

            except ValueError:
                id2 = ""

            lat = float(line[12:20])
            lon = float(line[21:30])
            alt = float(line[31:37])
            state = line[38:40]
            name = line[41:71]
            start = int(line[72:76])
            end = int(line[77:81])
            count = int(line[82:88])
            out.loc[i] = (id, id2, lat, lon, alt, state, name, start, end, count)

        print("Data processed " + str(i))
        out.loc[out.lon <= -998.8, 'lon'] = np.nan  # repalce missing values
        out.loc[out.alt <= -998.8, 'alt'] = np.nan  # repalce missing values
        out.loc[out.lat <= -98.8, 'lat'] = np.nan  # replace missing values
        out['name'] = out.name.str.strip()
        # out = out.set_index('id')
        if save:
            out.to_csv('igra2-station-list.csv',index=None)
        return out
    if '.csv' in filename:
        out = pd.read_csv(filename)
        print("Igra2StationList Data read from: " + filename)
        return out


def readChinaMetList(filename,save=False):
    """Read Chine met station list

    Args:
        filename: filename of station list
        save: save  file which end with '.npy' as csv file

    Returns:
        station informations
    """

    if '.npy' in filename:
        met_sites = np.load('met_sites.npy')
        header = ['province', 'id_met', 'city', 'lat', 'lon', 'alt_sensor', 'alt_site']
        out = pd.DataFrame(met_sites, columns=header)
        if save:
            out.to_csv('ChineMetSites.csv', index=None, encoding='utf_8_sig')
    if '.csv'in filename:
        out = pd.read_csv(filename)
    print("ChinaMetList Data read from: " + filename)
    return out

def readCmonocList(filename,save=False):
    """Read Cmonoc station list

    Args:
        filename: filename of station list
        save: save  file which end with '.txt' as csv file

    Returns:
        station informations

    """
    if '.txt' in filename:
        try:
            infile = open(filename)
            tmp = infile.read()
            data = tmp.splitlines()
            print("CmonocList Data read from: " + filename)
        except IOError as e:
            print("File not found: " + filename)
            raise e
        else:
            infile.close()

        out = pd.DataFrame(columns=['id_cmonoc', 'lat', 'lon', 'alt'])
        for i, line in enumerate(data):
            line = line.split()
            sitename, lat, lon, alt = [line[0], line[4], line[5], line[6]]
            out.loc[i] = (sitename, lat, lon, alt)
        # out = out.set_index('sitename')
        if save:
            out.to_csv('cmonocSites.csv', index=None)
        return out
    if '.csv' in filename:
        out = pd.read_csv(filename)
        print("CmonocList Data read from: " + filename)
        return out

def igraMatchMet(igraList, ChinaMetList, save=True):
    """igra station match China Met station

    Args:
        igraList: igra station informations
        ChinaMetList: China Met station informations
        save: save Matched station informations as csv

    Returns:
        Matched station informations

    """

    df_merge = pd.merge(igraList, ChinaMetList, left_on='wmo', right_on='id_met', how="left")
    df_merge = df_merge.dropna(axis=0, how='any')
    df_merge = df_merge[df_merge['end'] >= 2010]
    ext_header = [ 'id_met', 'province','city', 'id_igra', 'start', 'end']
    df_merge = df_merge.reindex(columns=ext_header)
    if save:
        df_merge.to_csv('igraMatchMet.csv', index=None, encoding='utf_8_sig')
    return df_merge
def MergeIgraMetCmonoc(igraList,ChinaMetList,cmonocList,save=True):
    """Three station matching

    Args:
        igraList: igra station informations
        ChinaMetList: China Met station informations
        cmonocList: cmonoc station informations
        save: save Matched station informations as csv

    Returns:
        Matched station informations

    """
    df_merge = pd.merge(igraList, ChinaMetList, left_on='wmo', right_on='id_met', how="left")
    df_merge = df_merge.dropna(axis=0, how='any')
    df_merge = pd.concat([df_merge, pd.DataFrame(columns=['id_cmonoc', 'lat_z', 'lon_z', 'distance'])])
    for index, row in df_merge.iterrows():
        # cmonoc匹配met坐标
        latMet = row["lat_y"]
        lonMet = row["lon_y"]
        curr_dis = ((latMet - cmonocList.lat) ** 2 + (lonMet - cmonocList.lon) ** 2) ** 0.5
        # 最小距离
        minDis = float(format(curr_dis.min(), '.4f'))
        # 最小距离所在行索引
        minIndex = curr_dis.idxmin()
        # 最小距离所在行
        minRow = cmonocList.loc[minIndex][:-1]
        minId = minRow['id_cmonoc']
        minLat = float(format(minRow['lat'], '.4f'))
        minLon = float(format(minRow['lon'], '.4f'))
        df_merge.loc[index, ['id_cmonoc', 'lat_z', 'lon_z', 'distance']] = minId, minLat, minLon, minDis
    df_merge = df_merge.sort_values(by='distance')
    df_merge = df_merge[df_merge['end'] >= 2010]
    ext_header = ['id_igra', 'start', 'end',  'id_met', 'province','city', 'lat_y', 'lon_y','id_cmonoc', 'lat_z', 'lon_z', 'distance']
    df_merge = df_merge.reindex(columns=ext_header)
    if save:
        df_merge.to_csv('Match_igra_met_cmonoc.csv',index=None, encoding='utf_8_sig')
    return df_merge
def metMatchCmonoc(ChinaMetList, cmonocList, save=True):
    """China Met station match cmonoc station

    Args:
        ChinaMetList: China Met station informations
        cmonocList: cmonoc station informations
        save: save Matched station informations as csv

    Returns:
        Matched station informations

    """
    merge = cmonocList.copy(deep=True)
    merge = merge.reindex(columns=['id_cmonoc', 'lat', 'lon','id_met','province', 'city','lat_y', 'lon_y', 'distance'])
    for index, row in cmonocList.iterrows():
        # cmonoc匹配met坐标
        latCmonoc = row["lat"]
        lonCmonoc = row["lon"]
        curr_dis = ((latCmonoc - ChinaMetList.lat) ** 2 + (lonCmonoc - ChinaMetList.lon) ** 2) ** 0.5
        # 最小距离
        minDis = float(format(curr_dis.min(), '.4f'))
        # 最小距离所在行索引
        minIndex = curr_dis.idxmin()
        # 最小距离所在行
        minRow = ChinaMetList.loc[minIndex][:-1]
        minId = minRow['id_met']
        minProvince = minRow['province']
        minCity = minRow['city']
        minLat = float(format(minRow['lat'], '.4f'))
        minLon = float(format(minRow['lon'], '.4f'))
        merge.loc[index, ['id_met','province', 'city','lat_y', 'lon_y', 'distance']] = minId, minProvince, minCity, minLat,minLon,minDis
    merge = merge.sort_values(by='distance')
    if save:
        merge.to_csv('metMatchCmonoc.csv',index=None, encoding='utf_8_sig')
    return merge


if __name__ == "__main__":
    # cmonocList = readCmonocList('CmonocSites.txt',save=True)
    # igraList = readIgra2StationList('igra2-station-list.txt',save=True)
    # ChinaMetList = readChinaMetList('met_sites.npy',save=True)
    ChinaMetList = readChinaMetList('ChineMetSites.csv')
    cmonocList = readCmonocList('CmonocSites.csv')
    igraList = readIgra2StationList('igra2-station-list.csv')
    met2cmonoc = metMatchCmonoc(ChinaMetList, cmonocList)
    igra2met = igraMatchMet(igraList, ChinaMetList)
    merge3 = MergeIgraMetCmonoc(igraList,ChinaMetList,cmonocList)


