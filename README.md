# ido-base


## todo:

    - [x] call api for new prj
    - [x] persistence to database
    - [x] deploy to serverless (Note Vercel only using python3.9)


## Cmds:

    - set DB_URI='postgres://'
    - uvicorn main:app --reload


## used-in-submodule:

```
    git submodule add https://github.com/arig42/ido-base base
    cp base/requirements.txt requirements.txt
    cp base/vercel.json vercel.json
    cp base/main.py main.py
```


## Notes:

    - https://fastapi.tiangolo.com/#example
