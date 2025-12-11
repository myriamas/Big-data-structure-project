"""Interface graphique Tkinter complète
Découvre automatiquement les fichiers *_schema.json
Charge les schémas et évalue les tailles de collections
Interface intuitive pour utilisateurs non-techniques"""

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
from bigdata.operators import (
    filter_with_sharding,
    filter_without_sharding,
    nested_loop_with_sharding,
    nested_loop_without_sharding,
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
            text="[Run]",
            style="Blue.TButton",
            command=self._run_computation,
        ).grid(row=2, column=2, padx=5, pady=5)

        ttk.Button(
            main,
            text="[Infer from sample]",
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

        # Advanced Options (subtle)
        options_frame = ttk.Frame(main)
        options_frame.grid(row=3, column=0, columnspan=4, sticky=tk.W, pady=(15, 5))
        
        ttk.Label(options_frame, text="Options:", font=("Segoe UI", 9), foreground="#888").pack(side=tk.LEFT)
        ttk.Button(options_frame, text="[Compare DBs]", command=self._show_db_comparison).pack(side=tk.LEFT, padx=5)
        ttk.Button(options_frame, text="[Full DB Size]", command=self._show_full_db_size).pack(side=tk.LEFT, padx=2)
        ttk.Button(options_frame, text="[Sharding Analysis]", command=self._show_sharding).pack(side=tk.LEFT, padx=2)
        ttk.Button(options_frame, text="[Operators]", command=self._show_operators_dialog).pack(side=tk.LEFT, padx=2)

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

    def _show_db_comparison(self):
        """Compare all DB1-DB5 variants"""
        self.output.delete(1.0, tk.END)
        self.output.insert(tk.END, "=== DB1-DB5 COMPARISON ===\n\n")
        self.output.insert(tk.END, "Run 'python Scripts/compute_db_sizes.py' in terminal for detailed comparison.\n\n")
        
        comparison_text = """DB1: Normalized (separate collections)
  - Product: 0.1132 GB
  - Stock: 25.3320 GB
  - OrderLine: 5900.8598 GB
  - Total: 5926.67 GB

DB2: Product + Stocks imbriqués
  - Total: 5901.36 GB

DB3: Stock as root + Product imbriqué
  - Total: 5926.55 GB

DB4: OrderLine as root + Product imbriqué
  - Total: 5926.55 GB

DB5: Product + OrderLines imbriqués (OPTIMAL)
  - Total: 25.87 GB

VERDICT: DB5 is 229x more efficient than DB1!
"""
        self.output.insert(tk.END, comparison_text)

    def _show_full_db_size(self):
        """Show full database size with all collections"""
        self.output.delete(1.0, tk.END)
        self.output.insert(tk.END, "=== FULL DATABASE SIZE ===\n\n")
        
        if not self.schema_var.get():
            messagebox.showwarning("Warning", "Please select a schema first.")
            return
        
        schema_path = self.schema_files[self.schema_combo.current()]
        schema = load_json(schema_path)
        
        try:
            doc_count = int(self.doc_count_var.get())
        except ValueError:
            messagebox.showerror("Error", "Document count must be a valid integer.")
            return
        
        collection = Collection(schema=schema, name=os.path.basename(schema_path), stats=self.stats, document_count=doc_count)
        
        output_text = f"Collection: {collection.name}\n"
        output_text += f"Documents: {doc_count:,}\n"
        doc_size = compute_document_size_from_schema(schema)
        coll_size_bytes = compute_collection_size_bytes(collection)
        coll_size_gb = bytes_to_gb(coll_size_bytes)
        
        output_text += f"\nDocument size: {doc_size} bytes\n"
        output_text += f"Collection size: {coll_size_gb:.4f} GB\n"
        output_text += f"Collection size: {coll_size_bytes:,} bytes\n"
        
        self.output.insert(tk.END, output_text)

    def _show_sharding(self):
        """Show sharding distribution analysis"""
        self.output.delete(1.0, tk.END)
        self.output.insert(tk.END, "=== SHARDING ANALYSIS ===\n\n")
        self.output.insert(tk.END, "Run 'python Scripts/compute_sharding_stats.py' in terminal for detailed analysis.\n\n")
        
        sharding_text = """Key Sharding Strategies (1000 servers):

Stock Collection:
  St - #IDP:  20,000 docs/server, 100 keys/server [EXCELLENT]
  St - #IDW:  20,000 docs/server, 0.2 keys/server [POOR - hotspots]

OrderLine Collection:
  OL - #IDC:  4M docs/server, 1,000 keys/server [GOOD]
  OL - #IDP:  4M docs/server, 100 keys/server [OK - watch hotspots]

Product Collection:
  Prod - #IDP:    100 docs/server, 100 keys/server [EXCELLENT]
  Prod - #brand:  100 docs/server, 5 keys/server [POOR]

RECOMMENDATION: Use #IDP for most collections (balanced distribution)
"""
        self.output.insert(tk.END, sharding_text)

    def _show_operators_dialog(self):
        """Open dialog to run operators and display results."""
        dlg = tk.Toplevel(self)
        dlg.title("Operators")
        dlg.geometry("640x360")

        frm = ttk.Frame(dlg, padding=12)
        frm.pack(fill=tk.BOTH, expand=True)

        ttk.Label(frm, text="Operator:", style="Body.TLabel").grid(row=0, column=0, sticky=tk.W)
        op_var = tk.StringVar(value="filter_with_sharding")
        op_combo = ttk.Combobox(frm, textvariable=op_var, state="readonly", width=30)
        op_combo['values'] = [
            'filter_with_sharding',
            'filter_without_sharding',
            'nested_loop_with_sharding',
            'nested_loop_without_sharding',
        ]
        op_combo.grid(row=0, column=1, sticky=tk.W, pady=4)

        ttk.Label(frm, text="Schema (left):", style="Body.TLabel").grid(row=1, column=0, sticky=tk.W)
        left_var = tk.StringVar()
        left_combo = ttk.Combobox(frm, textvariable=left_var, state="readonly", width=48)
        left_combo['values'] = [os.path.basename(p) for p in self.schema_files]
        left_combo.grid(row=1, column=1, sticky=tk.W, pady=4)

        ttk.Label(frm, text="Schema (right, for joins):", style="Body.TLabel").grid(row=2, column=0, sticky=tk.W)
        right_var = tk.StringVar()
        right_combo = ttk.Combobox(frm, textvariable=right_var, state="readonly", width=48)
        right_combo['values'] = [os.path.basename(p) for p in self.schema_files]
        right_combo.grid(row=2, column=1, sticky=tk.W, pady=4)

        ttk.Label(frm, text="Filtered key:", style="Body.TLabel").grid(row=3, column=0, sticky=tk.W)
        key_var = tk.StringVar()
        ttk.Entry(frm, textvariable=key_var, width=30).grid(row=3, column=1, sticky=tk.W, pady=4)

        ttk.Label(frm, text="Expected output keys (comma separated):", style="Body.TLabel").grid(row=4, column=0, sticky=tk.W)
        outkeys_var = tk.StringVar()
        ttk.Entry(frm, textvariable=outkeys_var, width=48).grid(row=4, column=1, sticky=tk.W, pady=4)

        ttk.Label(frm, text="Selectivity (optional 0.0-1.0):", style="Body.TLabel").grid(row=5, column=0, sticky=tk.W)
        sel_var = tk.StringVar()
        ttk.Entry(frm, textvariable=sel_var, width=12).grid(row=5, column=1, sticky=tk.W, pady=4)

        result_box = tk.Text(frm, height=10, bg="#ffffff", font=("Consolas", 10))
        result_box.grid(row=6, column=0, columnspan=2, sticky="nsew", pady=(8,0))
        frm.rowconfigure(6, weight=1)

        def _run_op():
            op = op_var.get()
            left_name = left_var.get()
            right_name = right_var.get()
            key = key_var.get().strip()
            outkeys = [k.strip() for k in outkeys_var.get().split(',') if k.strip()]
            selectivity = None
            if sel_var.get().strip():
                try:
                    selectivity = float(sel_var.get().strip())
                except Exception:
                    messagebox.showerror("Error", "Selectivity must be a number between 0 and 1.")
                    return

            if not left_name:
                messagebox.showwarning("Missing schema", "Please select a left schema.")
                return

            # Resolve schema paths
            left_path = None
            right_path = None
            for p in self.schema_files:
                if os.path.basename(p) == left_name:
                    left_path = p
                if os.path.basename(p) == right_name:
                    right_path = p

            try:
                left_schema = load_json(left_path) if left_path else {}
                right_schema = load_json(right_path) if right_path else {}
            except Exception as exc:
                messagebox.showerror("Error", f"Cannot load schema:\n{exc}")
                return

            # Create collection objects
            left_count = guess_default_doc_count(os.path.basename(left_path) if left_path else '', self.stats)
            right_count = guess_default_doc_count(os.path.basename(right_path) if right_path else '', self.stats)

            left_coll = Collection(name=os.path.basename(left_path) if left_path else 'left', schema=left_schema or {}, stats=self.stats, document_count=left_count)
            right_coll = Collection(name=os.path.basename(right_path) if right_path else 'right', schema=right_schema or {}, stats=self.stats, document_count=right_count)

            try:
                if op == 'filter_with_sharding':
                    res = filter_with_sharding(left_coll, outkeys, key, selectivity)
                elif op == 'filter_without_sharding':
                    res = filter_without_sharding(left_coll, outkeys, key, selectivity)
                elif op == 'nested_loop_with_sharding':
                    res = nested_loop_with_sharding(left_coll, right_coll, outkeys, key, selectivity, stats=self.stats)
                elif op == 'nested_loop_without_sharding':
                    res = nested_loop_without_sharding(left_coll, right_coll, outkeys, key, selectivity, stats=self.stats)
                else:
                    messagebox.showerror("Error", f"Unknown operator: {op}")
                    return
            except Exception as exc:
                messagebox.showerror("Error", f"Operator failed:\n{exc}")
                return

            # Pretty print result into result_box and main output
            result_box.delete('1.0', tk.END)
            import pprint
            result_box.insert(tk.END, pprint.pformat(res, indent=2))

            # Also push to main output for visibility
            self.output.delete('1.0', tk.END)
            self.output.insert(tk.END, f"Operator: {op}\n")
            self.output.insert(tk.END, pprint.pformat(res, indent=2))

        btn_frame = ttk.Frame(frm)
        btn_frame.grid(row=7, column=0, columnspan=2, sticky=tk.EW, pady=6)
        ttk.Button(btn_frame, text="Run", style="Blue.TButton", command=_run_op).pack(side=tk.RIGHT)
        ttk.Button(btn_frame, text="Close", command=dlg.destroy).pack(side=tk.RIGHT, padx=6)



if __name__ == "__main__":
    app = BigDataGui()
    app.mainloop()
