from sqlalchemy.orm import Session
from typing import Optional
from app.user.user_schema import User
from sqlalchemy import text

class UserRepository:
    def __init__(self, db:Session) -> None:
        self.db = db

    def get_user_by_email(self, email: str) -> Optional[User]:
        row = self.db.execute(
            text("SELECT email, password, username FROM users WHERE email=:e"),
            {"e": email},
        ).fetchone()

        if row is None:
            return None
        return User(email=row[0], password=row[1], username=row[2])

    def save_user(self, user: User) -> User: 
        exists = self.db.execute(
            text("SELECT 1 FROM users WHERE email=:e"),
            {"e": user.email},
        ).fetchone()

        if exists:
            self.db.execute(
                text("UPDATE users SET password=:p, username=:u WHERE email=:e"),
                {"e": user.email, "p": user.password, "u": user.username},
            )
        else:
            self.db.execute(
                text("INSERT INTO users (email, password, username) VALUES (:e,:p,:u)"),
                {"e": user.email, "p": user.password, "u": user.username},
            )

        self.db.commit()
        return user

    def delete_user(self, user: User) -> User:
        self.db.execute(
            text("DELETE FROM users WHERE email=:e"),
            {"e": user.email},
        )
        self.db.commit()
        return user