def handler(df):
    months = df['Month']
    for i, val in enumerate(months):
        months[i] = val + "-01"
