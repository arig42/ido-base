# ido-base

todo:

    - [x] call ape api for new prj
    - [x] persistence to database
    - [x] deploy to serverless (Note Vercel only using python3.9)


run:
    - set DB_URI='postgres://'
    - uvicorn main:app --reload


tutorial:

    - https://fastapi.tiangolo.com/#example


logs:

```
    pip install fastapi
    pip install "uvicorn[standard]"
    pip install psycopg2-binary
```