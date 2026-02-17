from flask import Blueprint, render_template, request
from ui.services.search_service import search_cars

main_bp = Blueprint("main", __name__)

@main_bp.route("/", methods=["GET", "POST"])
def search():
    results = []

    if request.method == "POST":
        filters = {
            "model_name": request.form.get("model_name"),
            "make_name": request.form.get("make_name"),
            "made_year": request.form.get("made_year"),
            "auction_city": request.form.get("auction_city"),
            "auction_date": request.form.get("auction_date"),
        }
        results = search_cars(filters)
    
    return render_template("search.html", results=results)