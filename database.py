
class database:
    def load_tableList(self, con):
        cursor = con.cursor()
        cursor.execute("SELECT name FROM sqlite_master WHERE type='table';")
        tables = cursor.fetchall()
        df_list = []
        for tname in tables:
            df_list.append(tname[0])
        return df_list
