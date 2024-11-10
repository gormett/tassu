import pandas as pd

# Read the CSV files
df_obce = pd.read_csv('obce_VT.csv', sep='|')
df_pop = pd.read_csv('statistics.sk/Z01_01_OK_SK041D_OB_pocet_obyvatelov.csv')
df_nar = pd.read_csv('statistics.sk/Z01_11_OK_SK041D_OB_narodnost.csv')
df_prac = pd.read_csv('statistics.sk/Z01_16_OK_SK041D_OB_pracujuci.csv')
df_pod = pd.read_csv('statistics.sk/Z01_17_OK_SK041D_OB_podnikatel.csv')

# Clean up obce names (fix escape sequences)
df_obce['Obec'] = df_obce['Obec'].str.replace('Obec\\+', '', regex=True)
df_obce['Obec'] = df_obce['Obec'].str.replace('Mesto\\+', '', regex=True)
df_obce['Obec'] = df_obce['Obec'].str.replace('\\+', ' ', regex=True)

# Process population data
df_pop = df_pop[['Územná jednotka', 'Spolu', 'muži (abs.)', 'ženy (abs.)']]
df_pop.columns = ['Obec', 'Obyvatelia_spolu', 'Muzi', 'Zeny']


# Process nationality data (keep only absolute columns)
nationality_cols = [col for col in df_nar.columns if '(abs.)' in col or col == 'Územná jednotka']
df_nar = df_nar[nationality_cols]
df_nar.columns = [col.replace(' (abs.)', '') for col in df_nar.columns]
df_nar = df_nar.rename(columns={'Územná jednotka': 'Obec'})


# Process employment data (keep only absolute columns)
employment_cols = [col for col in df_prac.columns if '(abs.)' in col or col == 'Územná jednotka']
df_prac = df_prac[employment_cols]
df_prac.columns = [col.replace(' (abs.)', '') for col in df_prac.columns]
df_prac = df_prac.rename(columns={'Územná jednotka': 'Obec'})


# Process entrepreneur data (keep only absolute columns)
entrepreneur_cols = [col for col in df_pod.columns if '(abs.)' in col or col == 'Územná jednotka']
df_pod = df_pod[entrepreneur_cols]
df_pod.columns = [col.replace(' (abs.)', '') for col in df_pod.columns]
df_pod = df_pod.rename(columns={'Územná jednotka': 'Obec'})

df_obce.merge(df_pop, on='Obec', how='inner').to_csv('output/statistics/obce_obyvatelia.csv', index=False)
df_obce.merge(df_nar, on='Obec', how='inner').to_csv('output/statistics/obce_narodnost.csv', index=False)
df_obce.merge(df_prac, on='Obec', how='inner').to_csv('output/statistics/obce_pracujuci.csv', index=False)
df_obce.merge(df_pod, on='Obec', how='inner').to_csv('output/statistics/obce_podnikatelia.csv', index=False)

