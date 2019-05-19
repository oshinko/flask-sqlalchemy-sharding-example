# Flask-SQLAlchemy Sharding Example

![Python 3](https://img.shields.io/badge/python-3-blue.svg)

This is an example application of Flask-SQLAlchemy horizontal sharding.


## Installing modules

```bash
ENV=tmp
python3 -m venv $HOME/.venv/$ENV
$HOME/.venv/$ENV/bin/python -m pip install -U pip -r requirements.txt
```


## Running server

```bash
FLASK_APP=app.py FLASK_DEBUG=1 $HOME/.venv/$ENV/bin/flask run
```


## Using APIs

Post your account.

```bash
ACCOUNT_ID=me
ACCOUNT_NAME=Me

curl -d id=$ACCOUNT_ID \
     -d name=$ACCOUNT_NAME \
     http://localhost:5000/accounts
```

Get account.

```bash
curl http://localhost:5000/accounts/$ACCOUNT_ID
```
