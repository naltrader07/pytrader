class Write_db:
    def append_db(self, con, tname, items):
        self.__dict__['cond'].acquire()
        cursor = con.cursor()
        sql = "INSERT INTO '{}'".format(tname) + 'VALUES(' + ','.join(['?'] * len(items)) + ')'
        print(sql)

        cursor.execute(sql, items)
        con.commit()
        self.__dict__['cond'].notify()
        self.__dict__['cond'].release()

    def delete_db(self, con, tname, tid):
        self.__dict__['cond'].acquire()
        cursor = con.cursor()
        sql = "DELETE FROM '{}' WHERE tid=?".format(tname)
        print(sql)

        cursor.execute(sql, (tid, ))
        con.commit()
        self.__dict__['cond'].notify()
        self.__dict__['cond'].release()

    def update_db(self, con, tname, tid, **kwargs):
        self.__dict__['cond'].acquire()
        cursor = con.cursor()
        sql = "UPDATE SET '{}'".format(tname)

        keys = []
        values = []
        for key, value in kwargs.items():
            keys.append('{}=?'.format(key))
            values.append(str(value))

        key = ','.join(keys)
        sql += key + "WHERE tid=?"
        print(sql)
        values.append(tid)

        cursor.execute(sql, values)
        con.commit()
        self.__dict__['cond'].notify()
        self.__dict__['cond'].release()

    def move_db(self, con, from_tname, to_tname, tid, items):
        self.append_db(con, to_tname, items)
        self.delete_db(con, from_tname, tid)
