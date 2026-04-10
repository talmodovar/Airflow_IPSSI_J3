#!/usr/bin/env python3
"""
Générateur de logs Apache Combined Log Format pour simulation e-commerce.
Usage : python3 generer_logs.py <date> <nb_lignes> <fichier_sortie>
Exemple : python3 generer_logs.py 2024-03-15 1000 /tmp/access_2024-03-15.log
"""
import random
import sys
from datetime import datetime, timedelta

IPS = [
    "92.184.12.44", "185.220.101.12", "78.23.145.67", "213.95.11.88",
    "5.188.10.132", "91.108.4.15", "176.31.208.51", "62.210.114.199",
    "37.187.0.200", "82.66.14.25", "90.54.12.144", "51.15.201.77",
    "109.190.122.6", "212.47.234.67", "163.172.0.100",
]

URLS = [
    ("GET", "/produit/smartphone-samsung-galaxy-s24-ultra", 200, 48200),
    ("GET", "/produit/laptop-dell-inspiron-15-amd", 200, 72100),
    ("GET", "/produit/casque-audio-sony-wh1000xm5", 200, 38900),
    ("GET", "/produit/aspirateur-dyson-v15", 200, 41000),
    ("GET", "/produit/montre-connectee-apple-watch-9", 200, 55300),
    ("GET", "/categorie/informatique", 200, 22400),
    ("GET", "/categorie/electromenager", 200, 19800),
    ("GET", "/categorie/smartphones", 200, 25100),
    ("GET", "/panier", 200, 8900),
    ("POST", "/checkout/valider", 200, 3400),
    ("POST", "/checkout/paiement", 200, 4100),
    ("GET", "/produit/article-retire-de-vente", 404, 512),
    ("GET", "/admin/dashboard", 403, 287),
    ("GET", "/produit/inexistant-xyz", 404, 512),
    ("POST", "/api/panier/ajouter", 500, 1024),
    ("GET", "/api/stock/verifier", 503, 890),
    ("GET", "/static/css/main.min.css", 200, 18200),
    ("GET", "/static/js/bundle.min.js", 200, 234100),
    ("GET", "/static/img/logo.png", 200, 4200),
    ("GET", "/favicon.ico", 200, 1150),
]

USER_AGENTS = [
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 Chrome/122.0.0.0 Safari/537.36",
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/605.1.15 Safari/605.1.15",
    "Mozilla/5.0 (iPhone; CPU iPhone OS 17_3 like Mac OS X) AppleWebKit/605.1.15 Mobile/15E148",
    "Mozilla/5.0 (Linux; Android 14; Pixel 8) AppleWebKit/537.36 Chrome/122.0.0.0 Mobile Safari/537.36",
    "Googlebot/2.1 (+http://www.google.com/bot.html)",
    "python-requests/2.31.0",
]

REFERRERS = [
    "https://www.google.fr/search?q=smartphone+pas+cher",
    "https://www.google.fr/search?q=pc+portable+dell",
    "https://www.marketplace.fr/",
    "https://www.marketplace.fr/categorie/informatique",
    "-",
    "-",
]

def generate_log_line(base_date):
    ip = random.choice(IPS)
    method, url, status, size = random.choice(URLS)
    # Ajouter une variation aléatoire sur la taille
    size = size + random.randint(-500, 500)
    if size < 0:
        size = 0
    user_agent = random.choice(USER_AGENTS)
    referrer = random.choice(REFERRERS)
    # Heure aléatoire dans la journée
    hour = random.randint(0, 23)
    minute = random.randint(0, 59)
    second = random.randint(0, 59)
    dt = base_date.replace(hour=hour, minute=minute, second=second)
    timestamp = dt.strftime("%d/%b/%Y:%H:%M:%S +0100")
    line = f'{ip} - - [{timestamp}] "{method} {url} HTTP/1.1" {status} {size} "{referrer}" "{user_agent}"'
    return line

def main():
    if len(sys.argv) != 4:
        print("Usage: python3 generer_logs.py <date YYYY-MM-DD> <nb_lignes> <fichier_sortie>")
        sys.exit(1)
    date_str = sys.argv[1]
    nb_lignes = int(sys.argv[2])
    fichier_sortie = sys.argv[3]
    base_date = datetime.strptime(date_str, "%Y-%m-%d")
    with open(fichier_sortie, "w") as f:
        for _ in range(nb_lignes):
            f.write(generate_log_line(base_date) + "\n")
    print(f"[OK] {nb_lignes} lignes générées dans {fichier_sortie}")

if __name__ == "__main__":
    main()
