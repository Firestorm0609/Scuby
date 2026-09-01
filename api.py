from flask import Flask, jsonify, request
from flask_caching import Cache

from flask_cors import CORS

from sqlalchemy import create_engine, Column, Integer, String, Text, Float, Boolean,  func, desc, text

from sqlalchemy.orm import sessionmaker, declarative_base, scoped_session

from sqlalchemy.sql import case

import math

import random

import hashlib

import string  # ✅ Added missing import

import os  # ✅ Added missing import

import time

import base64

import jwt

from nacl.signing import VerifyKey

from nacl.exceptions import BadSignatureError

from pumpfun.pumpfun_routes import pumpfun_bp
from datetime import datetime, timedelta



app = Flask(__name__)
cache = Cache(app, config={'CACHE_TYPE': 'simple', 'CACHE_DEFAULT_TIMEOUT': 180})

CORS(app, resources={r"/*": {"origins": "*"}})
app.register_blueprint(pumpfun_bp, url_prefix="/pumpfun")



Base = declarative_base()



# ============================================

# ⚙️ CONFIGURATION & DB SETUP

# ============================================



# ✅ FIX: Use current directory to avoid /root/ permission errors

BASE_DIR = os.path.dirname(os.path.abspath(__file__))

DATABASE_PATH = os.path.join(BASE_DIR, "vultvision.db")



# ✅ Security: Use Env Var or fallback to vibe123

ADMIN_SECRET = os.environ.get("ADMIN_SECRET", "vibe123")



engine = create_engine(f"sqlite:///{DATABASE_PATH}", echo=False, pool_pre_ping=True)

session_factory = sessionmaker(bind=engine)

Session = scoped_session(session_factory)



# ============================================

# 📦 DATABASE MODELS (Updated)

# ============================================


# ============================================
# ============================================

# ENDPOINT 1: Get hunter's reputation score
class Airdrop(Base):

    __tablename__ = "airdrops"

    id = Column(Integer, primary_key=True)

    name = Column(String(255))

    url = Column(String(500))

    type = Column(String(50))

    image = Column(String(500))

    conviction = Column(Integer, default=3)

    reason = Column(Text)

    guide = Column(Text)

    referral = Column(String(500))

    wallet = Column(String(100))

    hunter = Column(String(100), default="Anon")

    status = Column(String(20), default="pending")

    votes_up = Column(Integer, default=0)

    votes_down = Column(Integer, default=0)

    created_at = Column(String(50), default=lambda: str(datetime.now()))



    # ✅ FIX: Added Missing Columns for Success Tracker

    outcome = Column(String(20), default="pending")

    outcome_votes_confirmed = Column(Integer, default=0)

    outcome_votes_scam = Column(Integer, default=0)

    confirmed_at = Column(String(50))





class Comment(Base):

    __tablename__ = "comments"

    id = Column(Integer, primary_key=True)

    airdrop_id = Column(Integer)

    text = Column(Text)

    author = Column(String(100), default="Anon")

    user_id = Column(String(100))

    created_at = Column(String(50), default=lambda: str(datetime.now()))





class User(Base):

    __tablename__ = "users"

    id = Column(Integer, primary_key=True)

    wallet = Column(String(100), unique=True, index=True)

    username = Column(String(100))

    avatar = Column(String(500))

    bio = Column(Text)

    twitter = Column(String(100))

    discord = Column(String(100))

    created_at = Column(String(50), default=lambda: str(datetime.now()))



    # ✅ FIX: Added Missing Referral Column

    referral_code = Column(String(20), unique=True)





class Follow(Base):

    __tablename__ = "follows"

    id = Column(Integer, primary_key=True)

    follower_wallet = Column(String(100), index=True)

    following_wallet = Column(String(100), index=True)

    created_at = Column(String(50), default=lambda: str(datetime.now()))





class Watchlist(Base):

    __tablename__ = "watchlist"

    id = Column(Integer, primary_key=True)

    user_wallet = Column(String(100), index=True)

    airdrop_id = Column(Integer)

    created_at = Column(String(50), default=lambda: str(datetime.now()))





class XCalculation(Base):

    __tablename__ = "x_calculations"

    id = Column(Integer, primary_key=True)

    user_wallet = Column(String(100), index=True)

    x_username = Column(String(100))

    followers = Column(Integer)

    avg_impressions = Column(Integer)

    posts_per_day = Column(Float)

    engagement_rate = Column(Float)

    premium_follower_percent = Column(Float, default=8.0)

    us_audience_percent = Column(Float, default=30.0)

    estimated_monthly = Column(Float)

    estimated_yearly = Column(Float)

    potential_monthly = Column(Float)

    created_at = Column(String(50), default=lambda: str(datetime.now()))





class Referral(Base):

    __tablename__ = "referrals"

    id = Column(Integer, primary_key=True)

    referrer_wallet = Column(String(100), index=True)

    referee_wallet = Column(String(100), index=True)

    referral_code = Column(String(20))

    points_earned = Column(Integer, default=0)

    created_at = Column(String(50), default=lambda: str(datetime.now()))





class Tag(Base):

    __tablename__ = "tags"

    id = Column(Integer, primary_key=True)

    name = Column(String(50), unique=True, index=True)

    count = Column(Integer, default=0)





class AirdropTag(Base):

    __tablename__ = "airdrop_tags"

    id = Column(Integer, primary_key=True)

    airdrop_id = Column(Integer, index=True)

    tag_name = Column(String(50), index=True)





# Initialize Database

Base.metadata.create_all(engine)





@app.teardown_appcontext

def shutdown_session(exception=None):

    Session.remove()





# ============================================

# 🏠 HOME & HEALTH

# ============================================





@app.route("/", methods=["GET"])

def home():

    return jsonify(

        {

            "name": "VultVision API",

            "version": "8.1.0",

            "status": "online",

            "by": "Firestorm",

        }

    )





@app.route("/health", methods=["GET"])

def health_check():

    try:

        session = Session()

        session.execute(text("SELECT 1"))

        return jsonify({"status": "healthy", "db": "connected"})

    except Exception as e:

        return jsonify({"status": "dead", "error": str(e)}), 500





# ============================================

# 📋 AIRDROPS / FEED

# ============================================





@app.route("/airdrops", methods=["GET"])

def get_feed():

    try:

        session = Session()

        req_type = request.args.get("type", "all")

        search = request.args.get("search", "").strip().lower()

        hunter_filter = request.args.get("hunter", "").strip()

        page = int(request.args.get("page", 1))

        limit = int(request.args.get("limit", 50))

        offset = (page - 1) * limit



        if req_type == "pending":

            query = session.query(Airdrop).filter_by(status="pending")

        else:

            query = session.query(Airdrop).filter_by(status="approved")

            if req_type != "all":

                query = query.filter_by(type=req_type)



        # Search filter

        if search:

            query = query.filter(

                (func.lower(Airdrop.name).contains(search))

                | (func.lower(Airdrop.hunter).contains(search))

                | (func.lower(Airdrop.reason).contains(search))

            )



        # Hunter filter

        if hunter_filter:

            query = query.filter(

                (Airdrop.hunter == hunter_filter) | (Airdrop.wallet == hunter_filter)

            )



        drops = query.order_by(desc(Airdrop.id)).offset(offset).limit(limit).all()



        result = []

        for d in drops:

            c_count = session.query(Comment).filter_by(airdrop_id=d.id).count()

            result.append(

                {

                    "id": d.id,

                    "name": d.name,

                    "type": d.type,

                    "image": d.image,

                    "conviction": d.conviction,

                    "reason": d.reason,

                    "guide": d.guide,

                    "hunter": d.hunter,

                    "wallet": d.wallet,

                    "status": d.status,

                    "votes_up": d.votes_up,

                    "votes_down": d.votes_down,

                    "comment_count": c_count,

                    "url": d.url,

                    "created_at": d.created_at,

                    "outcome": d.outcome,  # ✅ Added Outcome

                }

            )



        return jsonify(result)

    except Exception as e:

        print(f"Feed error: {e}")

        return jsonify([]), 500





@app.route("/airdrop/<int:airdrop_id>", methods=["GET"])

def get_single_airdrop(airdrop_id):

    try:

        session = Session()

        drop = session.query(Airdrop).get(airdrop_id)

        if not drop:

            return jsonify({"error": "Not found"}), 404



        c_count = session.query(Comment).filter_by(airdrop_id=drop.id).count()

        return jsonify(

            {

                "id": drop.id,

                "name": drop.name,

                "type": drop.type,

                "image": drop.image,

                "conviction": drop.conviction,

                "reason": drop.reason,

                "guide": drop.guide,

                "hunter": drop.hunter,

                "wallet": drop.wallet,

                "status": drop.status,

                "votes_up": drop.votes_up,

                "votes_down": drop.votes_down,

                "comment_count": c_count,

                "url": drop.url,

                "created_at": drop.created_at,

                "outcome": drop.outcome,

                "outcome_confirmed": drop.outcome_votes_confirmed,

                "outcome_scam": drop.outcome_votes_scam,

            }

        )

    except Exception as e:

        return jsonify({"error": str(e)}), 500





@app.route("/submit", methods=["POST"])

def submit_alpha():

    try:

        session = Session()

        data = request.json

        url = data.get("url", "")

        logo = (

            data.get("image")

            or f"https://www.google.com/s2/favicons?domain={url}&sz=128"

        )



        new_drop = Airdrop(

            name=data.get("name"),

            url=url,

            type=data.get("type", "airdrop"),

            conviction=int(data.get("conviction", 3)),

            reason=data.get("reason"),

            guide=data.get("guide", ""),

            referral=data.get("referral", ""),

            wallet=data.get("wallet", ""),

            hunter=data.get("hunter", "Anon"),

            image=logo,

            status="pending",

            votes_up=0,

            votes_down=0,

        )

        session.add(new_drop)

        session.commit()



        # Auto-create user

        wallet = data.get("wallet")

        if wallet:

            existing = session.query(User).filter_by(wallet=wallet).first()

            if not existing:

                session.add(User(wallet=wallet, username=data.get("hunter", "Anon")))

                session.commit()



        return jsonify({"message": "Received", "id": new_drop.id}), 201

    except Exception as e:

        print(f"Submit error: {e}")

        session.rollback()

        return jsonify({"error": "Failed"}), 500





# ============================================

# 🗳️ VOTING & OUTCOMES

# ============================================





@app.route("/vote", methods=["POST"])

def vote():

    session = Session()

    try:

        data = request.json

        drop_id = data.get("id")

        direction = data.get("direction")



        drop = session.query(Airdrop).get(drop_id)

        if drop:

            if direction == "up":

                drop.votes_up = (drop.votes_up or 0) + 1

            elif direction == "down":

                drop.votes_down = (drop.votes_down or 0) + 1

            elif direction == "remove_up":

                drop.votes_up = max(0, (drop.votes_up or 0) - 1)

            elif direction == "remove_down":

                drop.votes_down = max(0, (drop.votes_down or 0) - 1)



            session.commit()

            return jsonify({"up": drop.votes_up, "down": drop.votes_down})

        return jsonify({"error": "404"}), 404

    except Exception as e:

        session.rollback()

        return jsonify({"error": str(e)}), 500





@app.route("/outcome/vote", methods=["POST"])

def vote_outcome():

    """Vote on whether alpha was legit or scam"""

    session = Session()

    try:

        data = request.json

        airdrop_id = data.get("airdrop_id")

        vote = data.get("vote")  # 'confirmed' or 'scam'

        wallet = data.get("wallet")



        if not wallet:

            return jsonify({"error": "Wallet required"}), 400



        drop = session.query(Airdrop).get(airdrop_id)

        if not drop:

            return jsonify({"error": "Not found"}), 404



        if vote == "confirmed":

            drop.outcome_votes_confirmed = (drop.outcome_votes_confirmed or 0) + 1

        elif vote == "scam":

            drop.outcome_votes_scam = (drop.outcome_votes_scam or 0) + 1



        # Auto-determine outcome if enough votes

        total_votes = (drop.outcome_votes_confirmed or 0) + (

            drop.outcome_votes_scam or 0

        )

        if total_votes >= 10:  # Threshold

            if (drop.outcome_votes_confirmed or 0) > (drop.outcome_votes_scam or 0):

                drop.outcome = "confirmed"

                drop.confirmed_at = str(datetime.now())

            else:

                drop.outcome = "scam"



        session.commit()

        return jsonify(

            {

                "success": True,

                "outcome": drop.outcome,

                "votes_confirmed": drop.outcome_votes_confirmed,

                "votes_scam": drop.outcome_votes_scam,

            }

        )

    except Exception as e:

        session.rollback()

        return jsonify({"error": str(e)}), 500





@app.route("/outcome/status/<int:airdrop_id>", methods=["GET"])

def get_outcome_status(airdrop_id):

    """Get outcome voting status"""

    try:

        session = Session()

        drop = session.query(Airdrop).get(airdrop_id)

        if not drop:

            return jsonify({"error": "Not found"}), 404



        return jsonify(

            {

                "outcome": drop.outcome or "pending",

                "votes_confirmed": drop.outcome_votes_confirmed or 0,

                "votes_scam": drop.outcome_votes_scam or 0,

                "confirmed_at": drop.confirmed_at,

            }

        )

    except Exception as e:

        return jsonify({"error": str(e)}), 500





@app.route("/outcomes/confirmed", methods=["GET"])

def get_confirmed_alphas():

    """Get all confirmed successful alphas"""

    try:

        session = Session()

        alphas = (

            session.query(Airdrop)

            .filter_by(outcome="confirmed")

            .order_by(desc(Airdrop.id))

            .limit(50)

            .all()

        )



        result = [

            {

                "id": a.id,

                "name": a.name,

                "type": a.type,

                "hunter": a.hunter,

                "confirmed_at": a.confirmed_at,

            }

            for a in alphas

        ]



        return jsonify(result)

    except Exception as e:

        return jsonify([]), 500





# ============================================

# 🏆 LEADERBOARD & STATS

# ============================================





@app.route("/leaderboard", methods=["GET"])

def get_leaderboard():

    try:

        session = Session()

        limit = int(request.args.get("limit", 20))



        hunters = (

            session.query(

                Airdrop.hunter,

                Airdrop.wallet,

                func.count(Airdrop.id).label("submitted"),

                func.sum(case((Airdrop.status == "approved", 1), else_=0)).label(

                    "approved"

                ),

                func.sum(Airdrop.votes_up).label("total_upvotes"),

                func.sum(Airdrop.votes_down).label("total_downvotes"),

            )

            .filter(

                Airdrop.hunter != "Anon", Airdrop.hunter != None, Airdrop.hunter != ""

            )

            .group_by(Airdrop.hunter, Airdrop.wallet)

            .all()

        )



        leaderboard = []

        for h in hunters:

            submitted = h.submitted or 0

            approved = int(h.approved or 0)

            upvotes = h.total_upvotes or 0

            downvotes = h.total_downvotes or 0



            points = (approved * 10) + (submitted * 1) + (upvotes * 2) - (downvotes * 1)

            win_rate = int((approved / submitted) * 100) if submitted > 0 else 0



            leaderboard.append(

                {

                    "hunter": h.hunter,

                    "wallet": h.wallet,

                    "submitted": submitted,

                    "approved": approved,

                    "points": max(0, points),

                    "winRate": win_rate,

                    "upvotes": upvotes,

                    "downvotes": downvotes,

                }

            )



        leaderboard.sort(key=lambda x: x["points"], reverse=True)



        for i, entry in enumerate(leaderboard[:limit]):

            entry["rank"] = i + 1



        return jsonify(leaderboard[:limit])

    except Exception as e:

        print(f"Leaderboard error: {e}")

        return jsonify([]), 500





@app.route("/user/stats", methods=["GET"])

def get_user_stats():

    try:

        session = Session()

        wallet = request.args.get("wallet")



        if not wallet:

            return jsonify({"error": "Wallet required"}), 400



        submitted_count = (

            session.query(Airdrop)

            .filter((Airdrop.wallet == wallet) | (Airdrop.hunter == wallet))

            .count()

        )



        approved_count = (

            session.query(Airdrop)

            .filter(

                ((Airdrop.wallet == wallet) | (Airdrop.hunter == wallet))

                & (Airdrop.status == "approved")

            )

            .count()

        )



        total_upvotes = (

            session.query(func.sum(Airdrop.votes_up))

            .filter((Airdrop.wallet == wallet) | (Airdrop.hunter == wallet))

            .scalar()

            or 0

        )



        points = (approved_count * 10) + (submitted_count * 1) + (total_upvotes * 2)

        win_rate = (

            int((approved_count / submitted_count) * 100) if submitted_count > 0 else 0

        )



        followers_count = (

            session.query(Follow).filter_by(following_wallet=wallet).count()

        )

        following_count = (

            session.query(Follow).filter_by(follower_wallet=wallet).count()

        )



        history = (

            session.query(Airdrop)

            .filter((Airdrop.wallet == wallet) | (Airdrop.hunter == wallet))

            .order_by(desc(Airdrop.id))

            .limit(10)

            .all()

        )



        history_list = [

            {

                "id": h.id,

                "name": h.name,

                "type": h.type,

                "status": h.status,

                "timestamp": h.created_at,

            }

            for h in history

        ]



        return jsonify(

            {

                "submitted": submitted_count,

                "approved": approved_count,

                "points": points,

                "winRate": win_rate,

                "upvotes": total_upvotes,

                "followers": followers_count,

                "following": following_count,

                "history": history_list,

            }

        )

    except Exception as e:

        print(f"Stats error: {e}")

        return jsonify({"submitted": 0, "points": 0, "winRate": 0}), 500





@app.route("/user/profile", methods=["GET", "POST"])

def handle_user_profile():

    session = Session()

    try:

        if request.method == "GET":

            wallet = request.args.get("wallet")

            user = session.query(User).filter_by(wallet=wallet).first()

            if not user:

                return jsonify(

                    {

                        "wallet": wallet,

                        "username": wallet[:8] + "..." if wallet else "Anon",

                        "avatar": None,

                        "exists": False,

                    }

                )

            return jsonify(

                {

                    "wallet": user.wallet,

                    "username": user.username,

                    "avatar": user.avatar,

                    "bio": user.bio,

                    "twitter": user.twitter,

                    "discord": user.discord,

                    "exists": True,

                }

            )



        if request.method == "POST":

            data = request.json

            wallet = data.get("wallet")

            if not wallet:

                return jsonify({"error": "Wallet required"}), 400



            user = session.query(User).filter_by(wallet=wallet).first()

            if not user:

                user = User(wallet=wallet)

                session.add(user)



            if "username" in data:

                user.username = data["username"]

            if "avatar" in data:

                user.avatar = data["avatar"]

            if "bio" in data:

                user.bio = data["bio"]

            if "twitter" in data:

                user.twitter = data["twitter"]



            session.commit()

            return jsonify({"success": True})



    except Exception as e:

        session.rollback()

        return jsonify({"error": str(e)}), 500





# ============================================

# 👥 FOLLOW SYSTEM

# ============================================





@app.route("/user/follow", methods=["POST"])

def follow_user():

    try:

        session = Session()

        data = request.json

        follower = data.get("follower_wallet")

        following = data.get("following_wallet")



        if not follower or not following:

            return jsonify({"error": "Missing wallet"}), 400

        if follower == following:

            return jsonify({"error": "Self follow"}), 400



        existing = (

            session.query(Follow)

            .filter_by(follower_wallet=follower, following_wallet=following)

            .first()

        )



        if existing:

            return jsonify({"error": "Already following"}), 400



        session.add(Follow(follower_wallet=follower, following_wallet=following))

        session.commit()

        return jsonify({"success": True})

    except Exception as e:

        session.rollback()

        return jsonify({"error": str(e)}), 500





@app.route("/user/unfollow", methods=["POST"])

def unfollow_user():

    try:

        session = Session()

        data = request.json

        follower = data.get("follower_wallet")

        following = data.get("following_wallet")



        follow = (

            session.query(Follow)

            .filter_by(follower_wallet=follower, following_wallet=following)

            .first()

        )



        if follow:

            session.delete(follow)

            session.commit()

            return jsonify({"success": True})



        return jsonify({"error": "Not following"}), 404

    except Exception as e:

        session.rollback()

        return jsonify({"error": str(e)}), 500





@app.route("/user/following", methods=["GET"])

def get_following():

    try:

        session = Session()

        wallet = request.args.get("wallet")

        follows = session.query(Follow).filter_by(follower_wallet=wallet).all()

        return jsonify(

            {"following": [f.following_wallet for f in follows], "count": len(follows)}

        )

    except Exception as e:

        return jsonify({"following": [], "error": str(e)}), 500





@app.route("/user/followers", methods=["GET"])

def get_followers():

    try:

        session = Session()

        wallet = request.args.get("wallet")

        followers = session.query(Follow).filter_by(following_wallet=wallet).all()

        return jsonify(

            {

                "followers": [f.follower_wallet for f in followers],

                "count": len(followers),

            }

        )

    except Exception as e:

        return jsonify({"followers": [], "error": str(e)}), 500





@app.route("/user/is-following", methods=["GET"])

def check_is_following():

    try:

        session = Session()

        follower = request.args.get("follower")

        following = request.args.get("following")



        exists = (

            session.query(Follow)

            .filter_by(follower_wallet=follower, following_wallet=following)

            .first()

            is not None

        )



        return jsonify({"isFollowing": exists})

    except Exception as e:

        return jsonify({"isFollowing": False}), 500





# ============================================

# 📌 WATCHLIST

# ============================================





@app.route("/watchlist", methods=["GET"])

def get_watchlist():

    try:

        session = Session()

        wallet = request.args.get("wallet")

        watchlist = session.query(Watchlist).filter_by(user_wallet=wallet).all()

        airdrop_ids = [w.airdrop_id for w in watchlist]



        if airdrop_ids:

            airdrops = session.query(Airdrop).filter(Airdrop.id.in_(airdrop_ids)).all()

            result = [

                {

                    "id": a.id,

                    "name": a.name,

                    "type": a.type,

                    "image": a.image,

                    "hunter": a.hunter,

                    "conviction": a.conviction,

                }

                for a in airdrops

            ]

        else:

            result = []



        return jsonify({"watchlist": result, "count": len(result)})

    except Exception as e:

        return jsonify({"watchlist": [], "error": str(e)}), 500





@app.route("/watchlist/add", methods=["POST"])

def add_to_watchlist():

    try:

        session = Session()

        data = request.json

        wallet = data.get("wallet")

        airdrop_id = data.get("airdrop_id")



        existing = (

            session.query(Watchlist)

            .filter_by(user_wallet=wallet, airdrop_id=airdrop_id)

            .first()

        )



        if existing:

            return jsonify({"error": "Already in watchlist"}), 400



        session.add(Watchlist(user_wallet=wallet, airdrop_id=airdrop_id))

        session.commit()

        return jsonify({"success": True})

    except Exception as e:

        session.rollback()

        return jsonify({"error": str(e)}), 500





@app.route("/watchlist/remove", methods=["POST"])

def remove_from_watchlist():

    try:

        session = Session()

        data = request.json

        wallet = data.get("wallet")

        airdrop_id = data.get("airdrop_id")



        item = (

            session.query(Watchlist)

            .filter_by(user_wallet=wallet, airdrop_id=airdrop_id)

            .first()

        )



        if item:

            session.delete(item)

            session.commit()

            return jsonify({"success": True})



        return jsonify({"error": "Not in watchlist"}), 404

    except Exception as e:

        session.rollback()

        return jsonify({"error": str(e)}), 500





@app.route("/watchlist/check", methods=["GET"])

def check_watchlist():

    try:

        session = Session()

        wallet = request.args.get("wallet")

        airdrop_id = request.args.get("airdrop_id")

        exists = (

            session.query(Watchlist)

            .filter_by(user_wallet=wallet, airdrop_id=airdrop_id)

            .first()

            is not None

        )

        return jsonify({"inWatchlist": exists})

    except Exception as e:

        return jsonify({"inWatchlist": False}), 500





# ============================================

# 💬 COMMENTS

# ============================================





@app.route("/comments", methods=["GET"])

def get_comments():

    session = Session()

    drop_id = request.args.get("id")

    comments = (

        session.query(Comment).filter_by(airdrop_id=drop_id).order_by(Comment.id).all()

    )

    return jsonify(

        [

            {

                "id": c.id,

                "author": c.author,

                "text": c.text,

                "user_id": c.user_id,

                "created_at": c.created_at,

            }

            for c in comments

        ]

    )





@app.route("/comment", methods=["POST"])

def post_comment():

    session = Session()

    try:

        data = request.json

        new_c = Comment(

            airdrop_id=data.get("id"),

            text=data.get("text"),

            author=data.get("author", "Anon"),

            user_id=data.get("user_id"),

        )

        session.add(new_c)

        session.commit()

        return jsonify({"success": True, "id": new_c.id})

    except Exception as e:

        session.rollback()

        return jsonify({"error": str(e)}), 500





@app.route("/comment/delete", methods=["POST"])

def delete_comment():

    session = Session()

    try:

        data = request.json

        comment_id = data.get("id")

        comment = session.query(Comment).get(comment_id)

        if comment:

            session.delete(comment)

            session.commit()

            return jsonify({"success": True})

        return jsonify({"error": "Not found"}), 404

    except Exception as e:

        session.rollback()

        return jsonify({"error": str(e)}), 500





# ============================================

# 🏷️ TAGS SYSTEM

# ============================================



DEFAULT_TAGS = [

    "DeFi",

    "Gaming",

    "L2",

    "Testnet",

    "Mainnet",

    "FreeMint",

    "Whitelist",

    "Token",

    "NFT",

    "DAO",

    "Bridge",

    "Staking",

    "Farming",

    "Social",

    "AI",

]





@app.route("/tags", methods=["GET"])

def get_all_tags():

    try:

        session = Session()

        tags = session.query(Tag).order_by(desc(Tag.count)).all()

        if not tags:

            return jsonify([{"name": t, "count": 0} for t in DEFAULT_TAGS])

        return jsonify([{"name": t.name, "count": t.count} for t in tags])

    except Exception as e:

        return jsonify([]), 500





@app.route("/tags/trending", methods=["GET"])

def get_trending_tags():

    try:

        session = Session()

        tags = session.query(Tag).order_by(desc(Tag.count)).limit(10).all()

        return jsonify([{"name": t.name, "count": t.count} for t in tags])

    except Exception as e:

        return jsonify([]), 500





@app.route("/tags/add", methods=["POST"])

def add_tags_to_airdrop():

    try:

        session = Session()

        data = request.json

        airdrop_id = data.get("airdrop_id")

        tags = data.get("tags", [])



        for tag_name in tags:

            tag_name = tag_name.strip().lower()

            if not tag_name:

                continue



            tag = session.query(Tag).filter_by(name=tag_name).first()

            if not tag:

                tag = Tag(name=tag_name, count=1)

                session.add(tag)

            else:

                tag.count += 1



            existing_link = (

                session.query(AirdropTag)

                .filter_by(airdrop_id=airdrop_id, tag_name=tag_name)

                .first()

            )



            if not existing_link:

                session.add(AirdropTag(airdrop_id=airdrop_id, tag_name=tag_name))



        session.commit()

        return jsonify({"success": True})

    except Exception as e:

        session.rollback()

        return jsonify({"error": str(e)}), 500





@app.route("/airdrop/<int:airdrop_id>/tags", methods=["GET"])

def get_airdrop_tags(airdrop_id):

    try:

        session = Session()

        tags = session.query(AirdropTag).filter_by(airdrop_id=airdrop_id).all()

        return jsonify([t.tag_name for t in tags])

    except Exception as e:

        return jsonify([]), 500





@app.route("/airdrops/by-tag/<tag_name>", methods=["GET"])

def get_airdrops_by_tag(tag_name):

    try:

        session = Session()

        tag_links = session.query(AirdropTag).filter_by(tag_name=tag_name.lower()).all()

        airdrop_ids = [t.airdrop_id for t in tag_links]



        if not airdrop_ids:

            return jsonify([])



        airdrops = (

            session.query(Airdrop)

            .filter(Airdrop.id.in_(airdrop_ids), Airdrop.status == "approved")

            .order_by(desc(Airdrop.id))

            .all()

        )



        result = [

            {

                "id": a.id,

                "name": a.name,

                "type": a.type,

                "hunter": a.hunter,

                "conviction": a.conviction,

                "image": a.image,

            }

            for a in airdrops

        ]



        return jsonify(result)

    except Exception as e:

        return jsonify([]), 500





# ============================================

# 🔥 HOT/TRENDING ALGORITHM

# ============================================





def calculate_hot_score(upvotes, downvotes, comments, created_at):

    try:

        if isinstance(created_at, str):

            created = datetime.fromisoformat(

                created_at.replace("Z", "+00:00").split(".")[0]

            )

        else:

            created = datetime.now()

        hours_old = (datetime.now() - created).total_seconds() / 3600

        engagement = (upvotes or 0) - (downvotes or 0) + (comments or 0) * 2

        decay = math.pow(hours_old + 2, 1.5)

        return round(engagement / decay, 4)

    except:

        return 0





@cache.cached(timeout=120, query_string=True)
@app.route("/airdrops/hot", methods=["GET"])

def get_hot_alphas():

    try:

        session = Session()

        limit = int(request.args.get("limit", 20))

        alphas = session.query(Airdrop).filter_by(status="approved").all()

        result = []

        for a in alphas:

            c_count = session.query(Comment).filter_by(airdrop_id=a.id).count()

            hot_score = calculate_hot_score(

                a.votes_up, a.votes_down, c_count, a.created_at

            )

            result.append(

                {

                    "id": a.id,

                    "name": a.name,

                    "type": a.type,

                    "image": a.image,

                    "conviction": a.conviction,

                    "hunter": a.hunter,

                    "votes_up": a.votes_up,

                    "votes_down": a.votes_down,

                    "comment_count": c_count,

                    "hot_score": hot_score,

                    "url": a.url,

                    "created_at": a.created_at,

                }

            )

        result.sort(key=lambda x: x["hot_score"], reverse=True)

        return jsonify(result[:limit])

    except Exception as e:

        print(f"Hot feed error: {e}")

        return jsonify([]), 500





@cache.cached(timeout=120, query_string=True)
@app.route("/airdrops/rising", methods=["GET"])

def get_rising_alphas():

    try:

        session = Session()

        limit = int(request.args.get("limit", 10))

        yesterday = str(datetime.now() - timedelta(hours=24))

        alphas = (

            session.query(Airdrop)

            .filter(Airdrop.status == "approved", Airdrop.created_at >= yesterday)

            .all()

        )



        result = []

        for a in alphas:

            c_count = session.query(Comment).filter_by(airdrop_id=a.id).count()

            engagement = (a.votes_up or 0) + (c_count * 2)

            result.append(

                {

                    "id": a.id,

                    "name": a.name,

                    "type": a.type,

                    "image": a.image,

                    "conviction": a.conviction,

                    "hunter": a.hunter,

                    "votes_up": a.votes_up,

                    "comment_count": c_count,

                    "engagement": engagement,

                    "created_at": a.created_at,

                }

            )

        result.sort(key=lambda x: x["engagement"], reverse=True)

        return jsonify(result[:limit])

    except Exception as e:

        return jsonify([]), 500





@cache.cached(timeout=120, query_string=True)
@app.route("/airdrops/top", methods=["GET"])

def get_top_alphas():

    try:

        session = Session()

        limit = int(request.args.get("limit", 20))

        period = request.args.get("period", "all")

        query = session.query(Airdrop).filter_by(status="approved")



        if period == "day":

            cutoff = str(datetime.now() - timedelta(days=1))

            query = query.filter(Airdrop.created_at >= cutoff)

        elif period == "week":

            cutoff = str(datetime.now() - timedelta(days=7))

            query = query.filter(Airdrop.created_at >= cutoff)

        elif period == "month":

            cutoff = str(datetime.now() - timedelta(days=30))

            query = query.filter(Airdrop.created_at >= cutoff)



        alphas = query.order_by(desc(Airdrop.votes_up)).limit(limit).all()

        result = [

            {

                "id": a.id,

                "name": a.name,

                "type": a.type,

                "image": a.image,

                "conviction": a.conviction,

                "hunter": a.hunter,

                "votes_up": a.votes_up,

                "votes_down": a.votes_down,

                "url": a.url,

            }

            for a in alphas

        ]

        return jsonify(result)

    except Exception as e:

        return jsonify([]), 500





# ============================================

# 🤝 REFERRAL SYSTEM

# ============================================





def generate_referral_code(wallet):

    hash_input = wallet + "".join(random.choices(string.ascii_uppercase, k=4))

    return hashlib.md5(hash_input.encode()).hexdigest()[:8].upper()





@app.route("/referral/code", methods=["GET"])

def get_referral_code_endpoint():

    try:

        session = Session()

        wallet = request.args.get("wallet")

        if not wallet:

            return jsonify({"error": "Wallet required"}), 400



        user = session.query(User).filter_by(wallet=wallet).first()



        # Ensure user exists first

        if not user:

            user = User(wallet=wallet)

            session.add(user)



        if not user.referral_code:

            user.referral_code = generate_referral_code(wallet)

            session.commit()

            # Re-fetch after commit to ensure object is refreshed

            user = session.query(User).filter_by(wallet=wallet).first()



        referral_link = f"https://vultvision.me?ref={user.referral_code}"



        return jsonify({"code": user.referral_code, "link": referral_link})

    except Exception as e:

        return jsonify({"error": str(e)}), 500





@app.route("/referral/apply", methods=["POST"])

def apply_referral_endpoint():

    try:

        session = Session()

        data = request.json

        referee_wallet = data.get("wallet")

        referral_code = data.get("code")



        if not referee_wallet or not referral_code:

            return jsonify({"error": "Missing data"}), 400



        referrer = (

            session.query(User).filter_by(referral_code=referral_code.upper()).first()

        )

        if not referrer:

            return jsonify({"error": "Invalid code"}), 404



        if referrer.wallet == referee_wallet:

            return jsonify({"error": "Self referral"}), 400



        existing = (

            session.query(Referral).filter_by(referee_wallet=referee_wallet).first()

        )

        if existing:

            return jsonify({"error": "Already referred"}), 400



        new_referral = Referral(

            referrer_wallet=referrer.wallet,

            referee_wallet=referee_wallet,

            referral_code=referral_code,

            points_earned=0,

        )

        session.add(new_referral)

        session.commit()

        return jsonify({"success": True, "referrer": referrer.wallet})

    except Exception as e:

        session.rollback()

        return jsonify({"error": str(e)}), 500





@app.route("/referral/stats", methods=["GET"])

def get_referral_stats():

    try:

        session = Session()

        wallet = request.args.get("wallet")

        referral_count = (

            session.query(Referral).filter_by(referrer_wallet=wallet).count()

        )

        total_points = (

            session.query(func.sum(Referral.points_earned))

            .filter_by(referrer_wallet=wallet)

            .scalar()

            or 0

        )

        recent = (

            session.query(Referral)

            .filter_by(referrer_wallet=wallet)

            .order_by(desc(Referral.id))

            .limit(10)

            .all()

        )

        recent_list = [

            {

                "referee": r.referee_wallet[:8] + "...",

                "points": r.points_earned,

                "date": r.created_at,

            }

            for r in recent

        ]

        return jsonify(

            {

                "total_referrals": referral_count,

                "total_points": total_points,

                "recent": recent_list,

            }

        )

    except Exception as e:

        return jsonify({"error": str(e)}), 500





@app.route("/referral/leaderboard", methods=["GET"])

def referral_leaderboard():

    try:

        session = Session()

        limit = int(request.args.get("limit", 10))

        top_referrers = (

            session.query(

                Referral.referrer_wallet,

                func.count(Referral.id).label("count"),

                func.sum(Referral.points_earned).label("points"),

            )

            .group_by(Referral.referrer_wallet)

            .order_by(desc("count"))

            .limit(limit)

            .all()

        )

        result = [

            {"wallet": r.referrer_wallet, "referrals": r.count, "points": r.points or 0}

            for r in top_referrers

        ]

        return jsonify(result)

    except Exception as e:

        return jsonify([]), 500





# ============================================

# 💰 X MONETIZATION CALCULATOR

# ============================================



RPM_BY_NICHE = {

    "crypto": 3.50,

    "finance": 3.00,

    "tech": 2.50,

    "business": 2.00,

    "general": 1.50,

    "entertainment": 1.00,

    "sports": 0.80,

    "lifestyle": 1.20,

    "gaming": 1.30,

    "news": 1.80,

}





def generate_personalized_tips(

    followers,

    avg_impressions,

    posts_per_day,

    engagement_rate,

    niche,

    us_audience_percent,

    premium_percent,

    estimated_monthly,

):

    tips = []



    # Clean up niche text

    niche_name = str(niche).title() if niche else "Content"



    # --- 1. ENGAGEMENT STRATEGY (Randomized) ---

    if engagement_rate < 1.5:

        # Critical urgency

        tips.append(

            random.choice(

                [

                    {

                        "tip": "🚨 The Algorithm is Burying You",

                        "impact": "Your engagement is critical (<1.5%). Stop posting external links in main posts; put them in replies.",

                    },

                    {

                        "tip": "⚠️ Ghost Ban Risk: High",

                        "impact": "Low interaction signals 'spam' to the AI. Reply to every single comment on your posts for 7 days to reset.",

                    },

                    {

                        "tip": "🪝 Your Hooks are Weak",

                        "impact": "Users are scrolling past. Start every post with a controversial statement or a hard number.",

                    },

                ]

            )

        )

    elif engagement_rate < 4.0:

        # Growth hacks

        tips.append(

            random.choice(

                [

                    {

                        "tip": "🚀 Farm Engagement with 'Autoplugging'",

                        "impact": "Setup a tool: If a post hits 100 likes, auto-reply with your offer link. Don't waste the traffic.",

                    },

                    {

                        "tip": "📊 Polls = Free Impressions",

                        "impact": f"Polls count as clicks. Run a divisive poll related to {niche_name} vs. a competitor.",

                    },

                    {

                        "tip": "🧗‍♂️ The 'Reply-Guy' Strategy",

                        "impact": f"Turn on notifications for big {niche_name} accounts. Be the first to comment with value.",

                    },

                ]

            )

        )

    else:

        # Scaling

        tips.append(

            {

                "tip": "🔥 Engagement is Elite. Scale Volume.",

                "impact": "Your audience loves you. You can afford to post 2x more often without annoying them.",

            }

        )



    # --- 2. MONETIZATION (US + Premium) ---

    if us_audience_percent < 30:

        tips.append(

            random.choice(

                [

                    {

                        "tip": "🇺🇸 You're Sleeping on US Ad Revenue",

                        "impact": "US CPM is 300% higher. Schedule posts for 8:00 AM EST (New York Morning).",

                    },

                    {

                        "tip": "🌎 Pivot to Western Culture",

                        "impact": "Your current posting times miss the high-paying US/UK window. Shift schedule by 4 hours.",

                    },

                    {

                        "tip": "💸 Target High-GDP Regions",

                        "impact": "Write threads about topics trending in the US to capture higher RPM.",

                    },

                ]

            )

        )



    if premium_percent < 5:

        tips.append(

            random.choice(

                [

                    {

                        "tip": "💎 Chase the Blue Checks",

                        "impact": f"Only verified views pay. Interact with big verified accounts in {niche_name} to get noticed.",

                    },

                    {

                        "tip": "🤵 Content is too 'Low Brow'",

                        "impact": "Premium users want deep dives. Write longer form content to attract power users.",

                    },

                ]

            )

        )



    # --- 3. NICHE SPECIFIC (Dynamic) ---

    if "crypto" in niche_name.lower() or "web3" in niche_name.lower():

        tips.append(

            random.choice(

                [

                    {

                        "tip": "⚡ Crypto Speed Run",

                        "impact": "Being first to news pays 10x more than analysis. Turn on notifs for major exchanges.",

                    },

                    {

                        "tip": "📉 Bear Market Strategy",

                        "impact": "In red markets, post educational content. In green markets, post hype.",

                    },

                ]

            )

        )

    elif "tech" in niche_name.lower() or "dev" in niche_name.lower():

        tips.append(

            random.choice(

                [

                    {

                        "tip": "👨‍💻 Build in Public",

                        "impact": "Devs love seeing the 'messy middle'. Share your bugs and errors, not just the success.",

                    },

                    {

                        "tip": "🔧 Share Your Stack",

                        "impact": "Posts listing 'Tools I Use' have the highest bookmark rate in Tech.",

                    },

                ]

            )

        )



    # --- 4. VOLUME CHECK ---

    if posts_per_day < 2:

        tips.append(

            {

                "tip": "📉 You Are Invisible",

                "impact": "1 post/day isn't enough. The timeline moves too fast. Aim for 3 minimum.",

            }

        )



    # Shuffle results so the order is never the same

    random.shuffle(tips)



    # Return unique top tips

    return tips[:4]





@app.route("/x/calculate", methods=["POST"])

def calculate_x_payout():

    try:

        data = request.json

        followers = int(data.get("followers", 0))

        avg_impressions = int(data.get("avg_impressions", 0))

        posts_per_day = float(data.get("posts_per_day", 1))

        engagement_rate = float(data.get("engagement_rate", 2.0))

        niche = data.get("niche", "general")

        us_audience_percent = float(data.get("us_audience_percent", 30))

        premium_percent = float(data.get("premium_percent", 8))



        base_rpm = RPM_BY_NICHE.get(niche, 1.50)



        # Multipliers

        engagement_multiplier = max(0.5, min(2.0, 1 + ((engagement_rate - 2) * 0.15)))

        geo_multiplier = (us_audience_percent / 100) * 1.0 + (

            (100 - us_audience_percent) / 100

        ) * 0.4

        final_rpm = base_rpm * engagement_multiplier * geo_multiplier



        monthly_posts = posts_per_day * 30

        monthly_impressions = monthly_posts * avg_impressions

        premium_impressions = monthly_impressions * (premium_percent / 100)



        estimated_monthly = (premium_impressions / 1000) * final_rpm

        estimated_yearly = estimated_monthly * 12

        potential_yearly = estimated_yearly * 1.5



        tips = generate_personalized_tips(

            followers,

            avg_impressions,

            posts_per_day,

            engagement_rate,

            niche,

            us_audience_percent,

            premium_percent,

            estimated_monthly,

        )



        # Save

        if data.get("save") and data.get("wallet"):

            session = Session()

            calc = XCalculation(

                user_wallet=data.get("wallet"),

                x_username=data.get("username"),

                followers=followers,

                avg_impressions=avg_impressions,

                posts_per_day=posts_per_day,

                engagement_rate=engagement_rate,

                premium_follower_percent=premium_percent,

                us_audience_percent=us_audience_percent,

                estimated_monthly=estimated_monthly,

                estimated_yearly=estimated_yearly,

                potential_monthly=potential_yearly / 12,

            )

            session.add(calc)

            session.commit()



        return jsonify(

            {

                "success": True,

                "monthly_earnings": round(estimated_monthly, 2),

                "yearly_earnings": round(estimated_yearly, 2),

                "potential_yearly": round(potential_yearly, 2),

                "rpm": round(final_rpm, 2),

                "monthly_impressions": int(monthly_impressions),

                "yearly_impressions": int(monthly_impressions * 12),

                "tips": tips,

                "followers": followers,  # 👈 ADDED THIS

                "engagement_rate": engagement_rate,  # 👈 ADDED THIS

            }

        )

    except Exception as e:

        return jsonify({"error": str(e)}), 500





# ============================================

# 👮 ADMIN & RUN

# ============================================





@app.route("/admin/decide", methods=["POST"])

def admin_approve():

    session = Session()

    try:

        data = request.json

        drop_id = data.get("id")

        decision = data.get("decision")

        secret = data.get("secret")



        if secret != ADMIN_SECRET:

            return jsonify({"error": "Unauthorized"}), 401



        drop = session.query(Airdrop).get(drop_id)

        if not drop:

            return jsonify({"error": "404"}), 404



        if decision == "approve":

            drop.status = "approved"

        elif decision == "reject":

            drop.status = "rejected"



        session.commit()

        return jsonify({"success": True})

    except Exception as e:

        session.rollback()

        return jsonify({"error": str(e)}), 500





@app.route("/admin/stats", methods=["GET"])

def admin_stats():

    try:

        session = Session()

        return jsonify(

            {

                "total_airdrops": session.query(Airdrop).count(),

                "pending": session.query(Airdrop).filter_by(status="pending").count(),

                "approved": session.query(Airdrop).filter_by(status="approved").count(),

                "total_users": session.query(User).count(),

                "total_comments": session.query(Comment).count(),

                "total_follows": session.query(Follow).count(),

            }

        )

    except Exception as e:

        return jsonify({"error": str(e)}), 500





if __name__ == "__main__":

    print("=" * 50)

    print("🔥 VultVision API v8.1")

    print("📍 Running on http://0.0.0.0:8000")

    print(f"📁 Database: {DATABASE_PATH}")

    print("=" * 50)

    app.run(host="0.0.0.0", port=8000, debug=False)