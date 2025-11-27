from mongoengine import Document, CASCADE
from mongoengine import fields
from datetime import datetime


class Author(Document):
    name = fields.StringField(max_length=20)
    age = fields.IntField(blank=True, null=True)
    created = fields.DateTimeField(default=datetime.now)

    def __str__(self):
        return self.name


class Post(Document):
    author = fields.ReferenceField(Author, null=True, reverse_delete_rule=CASCADE)
    name = fields.StringField(max_length=20)
    created = fields.DateTimeField(default=datetime.now)

    def __str__(self):
        return self.name
