# SQL-auto-constraints

Type `python sample.py` to create engines, insert data, and drop that table.

Remember to create database 'test' before executing this `sample.py`.

The main codes are in `my_sqlalchemy.py`.

### 12/21 - Some Informations

- 怎麼傳資料給我們的 function？

- 為了 check constraints，有兩個地方要再多做動作：

    - create: 多 create 一個 analysis 的 table
    - insert / update / delete: 去改動這個 analysis 的 table 的內容

- 怎麼 check constraints？

    - NOT NULL: maintain null counts

        - create: 多一個 column - "null_cnt"
        - insert:
            - 沒有被 insert 的 columns -> += len(rows) (maybe default not NULL)
            - 要被 insert 的 columns -> += len(value == "NULL")
            (還是先被資料 insert 到 temp table，再數 NULL counts?)
        - delete:
            - 所有 columns -> -= len(value == "NULL")

    - ...
