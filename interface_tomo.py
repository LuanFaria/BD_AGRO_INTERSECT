import tkinter as tk
from tkinter import ttk, messagebox
import os
from dotenv import load_dotenv
from upload_intersect_ndvi import upload_intersect_ndvi

# ==========================================
#  Carrega variáveis de ambiente do .env
# ==========================================
load_dotenv()

def get_db_config():
    """Lê as variáveis de ambiente do .env e retorna o dicionário de conexão."""
    return {
        "dbname": os.getenv("DATABASE_RDS"),
        "user": os.getenv("USER_RDS"),
        "password": os.getenv("PASSWORD_RDS"),
        "host": os.getenv("HOST_RDS"),
        "port": os.getenv("PORT_RDS")
    }

# ==========================================
#  Interface principal - Upload INTERSECT_NDVI
# ==========================================
root = tk.Tk()
root.title("📤 Upload INTERSECT_NDVI")
root.geometry("500x420")
root.configure(bg="#f0f2f5")
root.resizable(False, False)

# Cabeçalho
header = tk.Frame(root, bg="#0078D7", height=70)
header.pack(fill="x")
tk.Label(
    header,
    text="📤 Upload INTERSECT_NDVI",
    bg="#0078D7",
    fg="white",
    font=("Segoe UI Semibold", 16)
).pack(pady=15)

# Corpo
body = tk.Frame(root, bg="#f0f2f5")
body.pack(fill="both", expand=True, padx=30, pady=(10, 0))

tk.Label(
    body,
    text="Preencha as informações abaixo para subir o shapefile NDVI:",
    font=("Segoe UI", 10),
    bg="#f0f2f5",
    fg="#333"
).pack(pady=(0, 20))

card = tk.Frame(body, bg="white", relief="groove", bd=1, padx=20, pady=20)
card.pack(fill="x")

# Campos
tk.Label(card, text="🧾 ID do Cliente:", font=("Segoe UI", 10, "bold"), bg="white").grid(row=0, column=0, pady=10, sticky="e")
client_id_entry = ttk.Entry(card, width=25)
client_id_entry.grid(row=0, column=1, pady=10, padx=10)

tk.Label(card, text="🪟 Janela:", font=("Segoe UI", 10, "bold"), bg="white").grid(row=1, column=0, pady=10, sticky="e")
janela_var = tk.StringVar(value="J1")
janela_menu = ttk.OptionMenu(card, janela_var, "J1", "J1", "J2", "J3", "J4")
janela_menu.grid(row=1, column=1, pady=10, padx=10, sticky="w")

tk.Label(card, text="📅 Safra:", font=("Segoe UI", 10, "bold"), bg="white").grid(row=2, column=0, pady=10, sticky="e")
safra_var = tk.StringVar(value="2025")
safra_menu = ttk.OptionMenu(card, safra_var, "2025", "2023", "2024", "2025", "2026")
safra_menu.grid(row=2, column=1, pady=10, padx=10, sticky="w")

status_label = tk.Label(body, text="", bg="#f0f2f5", fg="#0078D7", font=("Segoe UI", 10, "bold"))
status_label.pack(pady=(10, 0))

# Função de upload
def execute_upload():
    try:
        db_config = get_db_config()
        clientes_id = int(client_id_entry.get())
        janela = janela_var.get()
        safra = safra_var.get()

        upload_intersect_ndvi(clientes_id, janela, safra, db_config)

        status_label.config(text="✅ Upload realizado com sucesso!", fg="#28a745")
        messagebox.showinfo("✅ Sucesso", "Upload realizado com sucesso!")
    except Exception as e:
        status_label.config(text=f"❌ Erro: {e}", fg="red")
        messagebox.showerror("❌ Erro", f"Ocorreu um erro: {e}")

# Botão principal
upload_btn = tk.Button(
    body,
    text="🚀 Subir Shapefile",
    command=execute_upload,
    bg="#0078D7",
    fg="white",
    font=("Segoe UI Semibold", 11),
    relief="flat",
    activebackground="#005A9E",
    activeforeground="white",
    padx=20,
    pady=10,
    cursor="hand2"
)
upload_btn.pack(pady=25)

root.mainloop()
