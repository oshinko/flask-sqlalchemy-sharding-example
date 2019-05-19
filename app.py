from datetime import datetime
from pathlib import Path

from flask import Flask, jsonify, request
from flask_marshmallow import Marshmallow
from flask_sharded_sqlalchemy import BindKeyPattern, ShardedSQLAlchemy

database_dir = Path.cwd()

default_database = 'sqlite:///{}'.format(database_dir / 'commons.db')
binds = {'accounts:0': 'sqlite:///{}'.format(database_dir / 'accounts.0.db'),
         'accounts:1': 'sqlite:///{}'.format(database_dir / 'accounts.1.db'),
         'asia': 'sqlite:///{}'.format(database_dir / 'asia.db')}

app = Flask(__name__)
app.config['SQLALCHEMY_DATABASE_URI'] = default_database
app.config['SQLALCHEMY_BINDS'] = binds
app.config['SQLALCHEMY_ECHO'] = True
app.config['SQLALCHEMY_TRACK_MODIFICATIONS'] = False

db = ShardedSQLAlchemy(app)
ma = Marshmallow(app)


class Metadata(db.Model):
    __tablename__ = 'metadata'
    key = db.Column(db.String(16), primary_key=True)
    value = db.Column(db.String(), nullable=False)
    created = db.Column(db.DateTime, nullable=False, default=datetime.utcnow)
    updated = db.Column(db.DateTime, nullable=True)


class AsianCity(db.Model):
    __bind_key__ = 'asia'
    __tablename__ = 'cities'
    id = db.Column(db.String(16), primary_key=True)
    name = db.Column(db.String(), nullable=False)
    population = db.Column(db.Integer, nullable=False)
    created = db.Column(db.DateTime, nullable=False, default=datetime.utcnow)
    updated = db.Column(db.DateTime, nullable=True)


class Account(db.Model):
    __bind_key__ = BindKeyPattern(r'accounts:\d+')
    __tablename__ = 'accounts'
    id = db.Column(db.String(16), primary_key=True)
    type = db.Column(db.String(16), nullable=False, default='personal')
    name = db.Column(db.String(), nullable=False)
    email = db.Column(db.String(), nullable=True)
    address = db.Column(db.String(), nullable=True)
    created = db.Column(db.DateTime, nullable=False, default=datetime.utcnow)
    updated = db.Column(db.DateTime, nullable=True)

    @classmethod
    def __hash_id__(cls, ident):
        return ord(ident[0][0])


class AccountSchema(ma.ModelSchema):
    class Meta:
        model = Account


@app.before_first_request
def init():
    db.create_all()

    # Update profiles
    region, city, population = 'asia', 'tokyo', 13839910

    entity = Metadata.query.get('region')
    if entity:
        entity.value = region
    else:
        entity = Metadata(key='region', value=region)
        db.session.add(entity)

    entity = AsianCity.query.get(city)
    if not entity:
        entity = AsianCity(id=city, name=city.title(), population=population)
        db.session.add(entity)

    db.session.commit()


@app.route('/')
def index():
    return jsonify('Hello!')


@app.route('/accounts/<aid>')
def get(aid):
    account = Account.query.get(aid)
    if account:
        return jsonify(AccountSchema().dump(account).data)
    return jsonify('Not Found'), 404


@app.route('/accounts', methods=['POST'])
def post():
    aid = request.form['id']
    account = Account()
    account.id = aid
    account.type = request.form.get('type')
    account.name = request.form['name']
    account.email = request.form.get('email')
    account.address = request.form.get('address')
    db.session.add(account)
    db.session.commit()
    return jsonify(AccountSchema().dump(account).data)


@app.route('/accounts/<aid>', methods=['DELETE'])
def delete(aid):
    account = Account.query.get(aid)
    if account:
        db.session.delete(account)
        db.session.commit()
        return jsonify(AccountSchema().dump(account).data)
    return jsonify('Not Found'), 404
