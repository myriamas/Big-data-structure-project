import os
import json
import tkinter as tk
from tkinter import ttk, messagebox, filedialog, simpledialog

from bigdata.models import Collection
from bigdata.sizes import (
    compute_document_size_from_schema,
    compute_collection_size_bytes,
    bytes_to_gb,
)

BASE_DIR = os.path.dirname(os.path.abspath(__file__))


def load_json(path):
    with open(path, "r", encoding="utf-8") as f:
        return json.load(f)


def discover_schema_files(schema_dir=None):
    if schema_dir is None:
        schema_dir = os.path.join(BASE_DIR, "schema")
    else:
        # si on passe "schema", on le rend relatif à AppCore
        if not os.path.isabs(schema_dir):
            schema_dir = os.path.join(BASE_DIR, schema_dir)

    if not os.path.isdir(schema_dir):
        return []

    files = []
    for name in os.listdir(schema_dir):
        if name.endswith("_schema.json"):
            files.append(os.path.join(schema_dir, name))
    files.sort()
    return files


def guess_default_doc_count(schema_filename, stats):
    fname = schema_filename.lower()
    nb_products = stats["nb_products"]
    nb_warehouses = stats["nb_warehouses"]
    nb_orderlines = stats["nb_orderlines"]

    if "stock" in fname:
        return nb_products * nb_warehouses
    if "orderline" in fname or "order_line" in fname or "ol" in fname:
        return nb_orderlines
    return nb_products


def infer_schema_from_sample(sample):
    if isinstance(sample, dict):
        properties = {}
        for key, value in sample.items():
            properties[key] = infer_schema_from_sample(value)
        return {
            "type": "object",
            "properties": properties,
            "required": list(properties.keys()),
        }

    if isinstance(sample, list):
        if len(sample) > 0:
            item_schema = infer_schema_from_sample(sample[0])
        else:
            item_schema = {}
        return {"type": "array", "items": item_schema}

    if isinstance(sample, bool):
        return {"type": "boolean"}
    if isinstance(sample, int):
        return {"type": "integer"}
    if isinstance(sample, float):
        return {"type": "number"}
    return {"type": "string"}


def wrap_schema(schema, title="InferredSchema"):
    if isinstance(schema, dict) and schema.get("type") == "object" and "properties" in schema:
        properties = schema["properties"]
        required = schema.get("required", list(properties.keys()))
    else:
        properties = {"root": schema}
        required = ["root"]

    return {
        "$schema": "https://json-schema.org/draft/2020-12/schema",
        "title": title,
        "type": "object",
        "properties": properties,
        "required": required,
    }


def is_json_schema(obj):
    if not isinstance(obj, dict):
        return False
    if obj.get("type") != "object":
        return False
    if "properties" not in obj:
        return False
    if not isinstance(obj["properties"], dict):
        return False
    return True


class BigDataGui(tk.Tk):
    def __init__(self):
        super().__init__()

        self.title("Big Data Structure Tool")
        self.geometry("900x520")
        self.configure(bg="#f9fbff")

        try:
            self.stats = load_json(os.path.join(BASE_DIR, "stats.json"))
        except Exception as exc:
            messagebox.showerror("Error", f"Cannot load stats.json:\n{exc}")
            self.destroy()
            return

        self.schema_files = discover_schema_files()


        self._configure_style()
        self._create_widgets()
        self._refresh_schema_list()

    def _configure_style(self):
        style = ttk.Style(self)
        # Try to use a more Windows-like theme if available
        try:
            if "vista" in style.theme_names():
                style.theme_use("vista")
            else:
                style.theme_use("clam")
        except tk.TclError:
            pass

        # Labels: only font, no custom background (to avoid bars/underlines)
        style.configure(
            "Body.TLabel",
            font=("Segoe UI", 10),
        )

        # Buttons
        style.configure(
            "TButton",
            font=("Segoe UI", 10),
            padding=5,
        )
        style.configure(
            "Blue.TButton",
            font=("Segoe UI", 10, "bold"),
            padding=6,
        )

    def _create_widgets(self):
        main = ttk.Frame(self, padding=15)
        main.pack(fill=tk.BOTH, expand=True)

        # Title without custom style/background
        title = ttk.Label(
            main,
            text="Big Data Structure – Storage Estimator",
            font=("Segoe UI", 14, "bold"),
        )
        title.grid(row=0, column=0, columnspan=4, sticky=tk.W, pady=(0, 15))

        ttk.Label(main, text="Schema:", style="Body.TLabel").grid(row=1, column=0, sticky=tk.W, pady=5)

        self.schema_var = tk.StringVar()
        self.schema_combo = ttk.Combobox(
            main,
            textvariable=self.schema_var,
            state="readonly",
            width=50,
        )
        self.schema_combo.grid(row=1, column=1, sticky=tk.W, pady=5)
        self.schema_combo.bind("<<ComboboxSelected>>", self._on_schema_changed)

        ttk.Button(main, text="Refresh", command=self._refresh_schema_list).grid(
            row=1, column=2, padx=5, pady=5
        )
        ttk.Button(main, text="Browse schema...", command=self._browse_schema).grid(
            row=1, column=3, padx=5, pady=5
        )

        ttk.Label(main, text="Document count:", style="Body.TLabel").grid(row=2, column=0, sticky=tk.W, pady=5)

        self.doc_count_var = tk.StringVar()
        ttk.Entry(main, textvariable=self.doc_count_var, width=25).grid(
            row=2, column=1, sticky=tk.W, pady=5
        )

        ttk.Button(
            main,
            text="Run",
            style="Blue.TButton",
            command=self._run_computation,
        ).grid(row=2, column=2, padx=5, pady=5)

        ttk.Button(
            main,
            text="Infer from sample...",
            command=self._infer_from_sample_dialog,
        ).grid(row=2, column=3, padx=5, pady=5)

        info = ttk.Label(
            main,
            text=(
                "Schemas are detected in the 'schema/' folder (files ending with '_schema.json'). "
                "You can also browse any JSON Schema file, or infer a schema automatically from a JSON sample."
            ),
            style="Body.TLabel",
            foreground="#606b7d",
            wraplength=820,
            justify="left",
        )
        info.grid(row=3, column=0, columnspan=4, sticky=tk.W, pady=(8, 5))

        ttk.Label(main, text="Output:", style="Body.TLabel").grid(row=4, column=0, sticky=tk.NW, pady=(10, 0))

        self.output = tk.Text(
            main,
            font=("Consolas", 10),
            bg="#eef3ff",
            height=18,
            relief="flat",
            bd=0,
            highlightthickness=0,
        )
        self.output.grid(row=4, column=1, columnspan=3, sticky="nsew", pady=5)

        main.rowconfigure(4, weight=1)
        main.columnconfigure(1, weight=1)

    def _refresh_schema_list(self):
        self.schema_files = discover_schema_files()

        names = [os.path.basename(p) for p in self.schema_files]
        self.schema_combo["values"] = names

        if names:
            self.schema_combo.current(0)
            self._update_doc_count(names[0])
        else:
            self.schema_combo.set("")
            self.doc_count_var.set("")

    def _update_doc_count(self, display_name):
        schema_path = None
        for p in self.schema_files:
            if os.path.basename(p) == display_name:
                schema_path = p
                break

        if schema_path is None:
            self.doc_count_var.set("")
            return

        default_docs = guess_default_doc_count(os.path.basename(schema_path), self.stats)
        self.doc_count_var.set(str(default_docs))

    def _on_schema_changed(self, event=None):
        name = self.schema_var.get()
        if name:
            self._update_doc_count(name)

    def _browse_schema(self):
        path = filedialog.askopenfilename(
            title="Select JSON Schema file",
            filetypes=[("JSON files", "*.json"), ("All files", "*.*")],
        )
        if not path:
            return

        if path not in self.schema_files:
            self.schema_files.append(path)
            names = [os.path.basename(p) for p in self.schema_files]
            self.schema_combo["values"] = names

        self.schema_var.set(os.path.basename(path))
        self._update_doc_count(os.path.basename(path))

    def _compute_and_display(self, collection, extra_header=None):
        try:
            doc_size = compute_document_size_from_schema(collection.schema)
            coll_bytes = compute_collection_size_bytes(collection)
            coll_gb = bytes_to_gb(coll_bytes)
        except Exception as exc:
            messagebox.showerror("Error", f"Error during computation:\n{exc}")
            return

        self.output.delete("1.0", tk.END)
        if extra_header:
            self.output.insert(tk.END, extra_header + "\n")
        self.output.insert(tk.END, f"Collection name: {collection.name}\n")
        self.output.insert(tk.END, f"Document count: {collection.document_count}\n")
        self.output.insert(tk.END, f"Document size (bytes): {doc_size}\n")
        self.output.insert(tk.END, f"Collection size (bytes): {coll_bytes}\n")
        self.output.insert(tk.END, f"Collection size (GB): {coll_gb}\n")

    def _run_computation(self):
        name = self.schema_var.get()
        if not name:
            messagebox.showwarning("No selection", "Please select a schema.")
            return

        schema_path = None
        for p in self.schema_files:
            if os.path.basename(p) == name:
                schema_path = p
                break

        if schema_path is None:
            messagebox.showerror("Error", "Selected schema path cannot be resolved.")
            return

        try:
            raw = load_json(schema_path)
        except Exception as exc:
            messagebox.showerror("Error", f"Cannot load JSON file:\n{exc}")
            return

        doc_count_str = self.doc_count_var.get().strip()
        try:
            document_count = int(doc_count_str)
        except ValueError:
            messagebox.showerror("Error", "Document count must be an integer.")
            return

        if not is_json_schema(raw):
            answer = messagebox.askyesno(
                "Fichier JSON détecté",
                "Ce fichier ne ressemble pas à un schéma JSON (JSON Schema).\n\n"
                "Voulez-vous inférer automatiquement un schéma à partir de ce JSON "
                "et calculer les tailles ?",
            )
            if not answer:
                return

            self._infer_from_sample_core(sample=raw, sample_path=schema_path, document_count=document_count)
            return

        collection = Collection(
            name=os.path.basename(schema_path),
            schema=raw,
            stats=self.stats,
            document_count=document_count,
        )
        header = f"Mode: JSON Schema\nSchema file: {schema_path}"
        self._compute_and_display(collection, extra_header=header)

    def _infer_from_sample_dialog(self):
        path = filedialog.askopenfilename(
            title="Select JSON sample file",
            filetypes=[("JSON files", "*.json"), ("All files", "*.*")],
        )
        if not path:
            return

        try:
            sample = load_json(path)
        except Exception as exc:
            messagebox.showerror("Error", f"Cannot load JSON file:\n{exc}")
            return

        self._infer_from_sample_core(sample=sample, sample_path=path)

    def _infer_from_sample_core(self, sample, sample_path, document_count=None):
        raw_schema = infer_schema_from_sample(sample)
        schema = wrap_schema(raw_schema, title="InferredSchema")

        if document_count is None:
            doc_count_str = simpledialog.askstring(
                "Document count",
                "Enter document count (integer):",
                initialvalue=str(self.stats.get("nb_products", 100000)),
                parent=self,
            )
            if doc_count_str is None:
                return
            try:
                document_count = int(doc_count_str.strip())
            except ValueError:
                messagebox.showerror("Error", "Document count must be an integer.")
                return

        collection = Collection(
            name="InferredFromSample",
            schema=schema,
            stats=self.stats,
            document_count=document_count,
        )

        header = (
            "Mode: inferred from sample\n"
            f"File: {sample_path}"
        )
        self._compute_and_display(collection, extra_header=header)


if __name__ == "__main__":
    app = BigDataGui()
    app.mainloop()
