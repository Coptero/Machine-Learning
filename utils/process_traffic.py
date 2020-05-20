import pandas as pd
from datetime import timedelta


def fillMissing(df, columns):
    """
    Fills traffic dataframe missing dates with 0 traffic registers.
    :param df: Dataframe containing traffic data.
    :param columns: List of dataframe columns.
    :return: traffic dataframe without missing dates.
    """
    df.reset_index(drop=True, inplace=True)
    dfSize = len(df.index)

    first = df.iloc[0]
    last = df.iloc[-1]
    firstDate = first["DT_MEASURE_DATETIME"]
    lastDate = last["DT_MEASURE_DATETIME"]
    rowCount = ((lastDate - firstDate).total_seconds() / 60) / 5  # Number of rows between those dates knowing that a row = 5 minutes

    if rowCount != dfSize:
        data = []  # List of dictionaries
        data.append(first.to_dict())
        index = 0
        for i in range(int(rowCount)):
            currentRow = data[-1]
            currentDate = currentRow.get("DT_MEASURE_DATETIME")
            if (currentDate.weekday()) < 5:
                currentRow['WEEKEND'] = False
            else:
                currentRow['WEEKEND'] = True
            currentDate = pd.to_datetime(currentDate, format="%Y-%m-%d %H:%M:%S")
            nextDate = df.loc[index + 1, "DT_MEASURE_DATETIME"]  # Get next row of dataframe to compare dates
            nextDate = pd.to_datetime(nextDate, format="%Y-%m-%d %H:%M:%S")

            dif = (nextDate - currentDate).total_seconds()
            if dif / 60 > 5:
                data.append({
                    columns[0]: currentRow[columns[0]],
                    columns[1]: currentRow[columns[1]],
                    columns[2]: currentRow[columns[2]],
                    columns[3]: 0,
                    columns[4]: 0,
                    columns[5]: currentDate + timedelta(seconds=300),
                    columns[6]: currentRow[columns[6]],
                    columns[7]: currentRow[columns[7]],
                    columns[8]: currentRow[columns[8]]
                })
            else:
                index += 1
                data.append(df.iloc[index].to_dict())
        return pd.DataFrame(data)
    else:
        return df


def get_interface_list(df):
    """
    Returns a dictionary containing the list of interfaces for each router
    in the dataframe.
    :param df: Dataframe containing traffic data.
    :return: Router-interfaces dictionary
    """
    interfacesDict = {}
    uniqueRouter = list(df['SC_ROUTER'].unique())
    for router in uniqueRouter:
        routerDF = df[df['SC_ROUTER'] == router]
        uniqueInterface = list(routerDF['DE_INTERFACE'].unique())
        interfacesDict.update({router: uniqueInterface})
    return interfacesDict


def initial_preprocess(df):
    """
    Preprocceses traffic dataframe.
    :param df: Dataframe containing traffic data.
    :return: Preprocessed DataFrame.
    """
    df.drop(new_df[new_df['DE_INTERFACE'].str.match('Tunnel')].index, inplace=True)
    df.drop(columns='DT_MEASURE_DATETIME_1', inplace=True)
    df["DT_MEASURE_DATETIME"] = pd.to_datetime(df["DT_MEASURE_DATETIME"], format="%Y%m%d%H%M%S")
    df["NU_SPEED"] = df["NU_SPEED"] / 1000000
    df['NU_TRAFFIC_INPUT'] = df['NU_TRAFFIC_INPUT'] / 1000000
    df['NU_TRAFFIC_OUTPUT'] = df['NU_TRAFFIC_OUTPUT'] / 1000000
    return df


if __name__ == "__main__":

    new_df = pd.read_csv('./SCOT-CAN-TOR-MW-U-370409.csv')

    new_df = initial_preprocess(new_df)
    columns = list(new_df.columns)
    auxDF = pd.DataFrame(columns=columns)
    new_df.reset_index(drop=True, inplace=True)

    interfacesDict = get_interface_list(new_df)

    for routerKey in interfacesDict:
        interfaceList = interfacesDict.get(routerKey)
        for interface in interfaceList:
            subDf = new_df.loc[(new_df['SC_ROUTER'] == routerKey) & (new_df['DE_INTERFACE'] == interface)]
            subDf.sort_values(by="DT_MEASURE_DATETIME", inplace=True)
            output = fillMissing(subDf, columns)
            auxDF = pd.concat([auxDF, output], ignore_index=True)

    auxDF.to_csv("./Procesados/SCOT-CAN-TOR-MW-U-370409.csv", index=False, encoding='utf-8-sig')
