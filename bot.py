# bot.py
from playwright.sync_api import sync_playwright, TimeoutError as PWTimeout
from dotenv import load_dotenv
from woocommerce import API
from pathlib import Path
import os, re, sys, time, urllib.parse, datetime, json, requests

# =======================
# CONFIG
# =======================
load_dotenv()

TENDA_URL       = "https://www.tendaatacado.com.br"
DEFAULT_TIMEOUT = int(os.getenv("PW_TIMEOUT", "60000"))

# Modo headless: False = mostra navegador, True = sem UI
HEADLESS        = True  # False para mostrar o navegador
SLOW_MO_MS      = int(os.getenv("PW_SLOWMO", "0"))

USE_CEP         = True
CEP_VALOR       = "05109-200"

# --- Lotes / Batching
BATCH_SIZE = int(os.getenv("BATCH_SIZE", "10"))

# --- CDS (ERP)
CDS_URL   = "http://63.143.45.98:800/"
CDS_USER  = os.getenv("CDS_USER", "hortigold")
CDS_PASS  = os.getenv("CDS_PASS", "hortigold@4120")
CLIENT_USER = os.getenv("CLIENT_USER", "marcela")
CLIENT_PASS = os.getenv("CLIENT_PASS", "1")
CLIENT_MODALIDADE_VALUE = os.getenv("CLIENT_MODALIDADE_VALUE", "2")  # 2 = Retaguarda

# --- WooCommerce / WP
WP_BASE_URL = (os.getenv("WP_BASE_URL") or "https://hortigold.com.br").rstrip("/")
WP_USER = os.getenv("WP_USER", "admin")
WP_PASS = os.getenv("WP_PASS", "figueiredo")

WOO_BASE_URL = (os.getenv("WOO_BASE_URL") or "").rstrip("/")
WOO_CK = os.getenv("WOO_CK")
WOO_CS = os.getenv("WOO_CS")
wc = None
if WOO_BASE_URL and WOO_CK and WOO_CS:
    wc = API(
        url=WOO_BASE_URL,
        consumer_key=WOO_CK,
        consumer_secret=WOO_CS,
        version="wc/v3",
        timeout=30,
        query_string_auth=False,
    )

# =======================
# LOG PATHS (ABSOLUTOS)
# =======================
BASE_DIR = Path(__file__).resolve().parent
LOG_DIR = Path(os.getenv("LOG_DIR", str(BASE_DIR / "logs")))

def get_log_filename():
    hoje = datetime.date.today().strftime("%Y-%m-%d")
    LOG_DIR.mkdir(parents=True, exist_ok=True)
    i = 1
    while True:
        fn = LOG_DIR / f"{hoje}_{i}.json"
        if not fn.exists():
            return str(fn)
        i += 1

def log_produto(sku, query, preco, status, log_file):
    data = []
    try:
        if os.path.exists(log_file):
            with open(log_file, "r", encoding="utf-8") as f:
                data = json.load(f)
    except Exception:
        pass
    data.append({
        "sku": sku,
        "produto": query,
        "preco": preco,
        "status": status,
        "hora": datetime.datetime.now().strftime("%H:%M:%S")
    })
    with open(log_file, "w", encoding="utf-8") as f:
        json.dump(data, f, ensure_ascii=False, indent=2)

# =======================
# HELPERS
# =======================
def is_page_closed(page):
    """Verifica se uma página está fechada de forma segura"""
    try:
        return (not page) or page.is_closed()
    except Exception:
        return True

def clean_price(text: str):
    if not text: return None
    t = re.sub(r"[^0-9,\.]", "", text)
    if "," in t: t = t.replace(".", "").replace(",", ".")
    try: return float(t)
    except: return None

def as_br_price(v: float) -> str:
    return f"{v:.2f}".replace(".", ",")

def log_step(msg, start=None):
    if start:
        print(f"[Tempo] {msg} ({time.time()-start:.2f}s)")
    else:
        print(f"[{msg}]")
    return time.time()

def chunked(seq, n):
    for i in range(0, len(seq), n):
        yield seq[i:i+n]

# =======================
# BROWSER/CONTEXT
# =======================
def make_browser_and_context(pw):
    ua = ("Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
          "(KHTML, like Gecko) Chrome/122.0.0.0 Safari/537.36")

    launch_args = [
        "--no-sandbox",
        "--disable-dev-shm-usage",
        "--disable-blink-features=AutomationControlled",
        "--lang=pt-BR",
    ]

    browser = pw.chromium.launch(
        headless=HEADLESS,  # Usa a variável HEADLESS da configuração
        slow_mo=SLOW_MO_MS,
        args=launch_args,
    )

    ctx_kwargs = dict(
        user_agent=ua,
        locale="pt-BR",
        timezone_id="America/Recife",
        viewport={"width": 1280, "height": 900},  # Sempre viewport fixo em headless
    )

    ctx = browser.new_context(**ctx_kwargs)

    # Bloqueia imagens, fontes e mídia para economizar recursos
    def _route(route):
        r = route.request
        if r.resource_type in {"image", "font", "media"}:
            return route.abort()
        return route.continue_()
    ctx.route("**/*", _route)

    ctx.add_init_script("Object.defineProperty(navigator,'webdriver',{get:()=>undefined});")
    return browser, ctx

def make_context_only(pw_browser):
    try:
        if not pw_browser:
            raise RuntimeError("Navegador não está disponível")
        
        ua = ("Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
              "(KHTML, like Gecko) Chrome/122.0.0.0 Safari/537.36")
        ctx_kwargs = dict(
            user_agent=ua,
            locale="pt-BR",
            timezone_id="America/Recife",
            viewport={"width": 1280, "height": 900},  # Sempre viewport fixo em headless
        )

        print("[Context] Criando contexto...")
        ctx = pw_browser.new_context(**ctx_kwargs)
        print("[Context] Contexto criado")

        # Bloqueia imagens, fontes e mídia para economizar recursos
        def _route(route):
            r = route.request
            if r.resource_type in {"image", "font", "media"}:
                return route.abort()
            return route.continue_()
        ctx.route("**/*", _route)

        ctx.add_init_script("Object.defineProperty(navigator,'webdriver',{get:()=>undefined});")
        
        # Adiciona o script do CEP se necessário (antes de criar páginas)
        if USE_CEP:
            ctx.add_init_script(build_cep_observer_js(CEP_VALOR))
        
        print("[Context] ✅ Contexto configurado com sucesso")
        return ctx
    except Exception as e:
        print(f"[Context] Erro ao criar contexto: {e}")
        import traceback
        traceback.print_exc()
        raise

def open_and_login_all(ctx):
    p_tenda = p_cds = p_wp = p_portal = None
    try:
        print("[Login] Verificando contexto...")
        if ctx is None:
            raise RuntimeError("Contexto inválido (None)")
        print("[Login] Contexto OK")
        
        # Criação das páginas
        print("[Login] Criando página Tenda...")
        p_tenda  = ctx.new_page()
        print("[Login] Página Tenda criada")
        
        print("[Login] Criando página CDS...")
        p_cds    = ctx.new_page()
        print("[Login] Página CDS criada")
        
        print("[Login] Criando página WP...")
        p_wp     = ctx.new_page()
        print("[Login] Página WP criada")
        
        print("[Login] Criando página Portal...")
        p_portal = ctx.new_page()
        print("[Login] Página Portal criada")
        
        print("[Login] Configurando timeouts...")
        for _p in (p_tenda, p_cds, p_wp, p_portal):
            _p.set_default_timeout(DEFAULT_TIMEOUT)
        print("[Login] Timeouts configurados")
            
        # Logins
        print("[Login] Acessando Tenda...")
        try:
            p_tenda.goto(TENDA_URL, wait_until="domcontentloaded", timeout=60000)
            print(f"[Login] Tenda carregada: {p_tenda.url}")
            p_tenda.wait_for_timeout(500)
            
            if USE_CEP:
                print("[Login] Configurando CEP...")
                ensure_cep(p_tenda, CEP_VALOR)
                nuke_overlays(p_tenda)
                print("[Login] CEP configurado")
        except Exception as tenda_err:
            print(f"[Login] ❌ Erro ao acessar Tenda: {tenda_err}")
            import traceback
            traceback.print_exc()
            raise
            
        print("[Login] Acessando CDS...")
        try:
            login_cds(p_cds)
        except Exception as cds_err:
            print(f"[Login] Erro crítico no login CDS: {cds_err}")
            # Verifica se a página ainda está viva
            try:
                if not is_page_closed(p_cds):
                    print(f"[Login] Página CDS ainda aberta, URL: {p_cds.url}")
                else:
                    print(f"[Login] Página CDS foi fechada!")
            except:
                print(f"[Login] Não foi possível verificar status da página CDS")
            raise
        
        print("[Login] Acessando WP...")
        try:
            wp_login(p_wp)
        except Exception as wp_err:
            print(f"[Login] ❌ Erro no login WP: {wp_err}")
            raise
        
        print("[Login] Acessando Portal...")
        try:
            login_portal(p_portal)
        except Exception as portal_err:
            print(f"[Login] ❌ Erro no login Portal: {portal_err}")
            raise
        
        print("[Login] ✅ Todos os logins concluídos com sucesso!")
        return p_tenda, p_cds, p_wp, p_portal
        
    except Exception as e:
        print(f"[Login] ❌ Erro ao criar páginas ou fazer login: {e}")
        import traceback
        traceback.print_exc()
        # Tenta fechar o que foi aberto se deu erro
        print("[Login] Limpando páginas criadas...")
        for _p in [p_tenda, p_cds, p_wp, p_portal]:
            try:
                if _p: 
                    print(f"[Login] Fechando página...")
                    _p.close()
            except Exception as close_err:
                print(f"[Login] Erro ao fechar página: {close_err}")
        raise e

# =======================
# Portal Hortigold
# =======================
PORTAL_URL = "https://mlovi.com.br/sistemahortigold/public/"
PORTAL_USER = os.getenv("PORTAL_USER", "admin")
PORTAL_PASS = os.getenv("PORTAL_PASS", "admin123")

def login_portal(page):
    try:
        page.goto(PORTAL_URL + "login.php", wait_until="domcontentloaded", timeout=60000)
        page.fill("#username", PORTAL_USER)
        page.fill("#password", PORTAL_PASS)
        try:
            with page.expect_navigation(wait_until="domcontentloaded", timeout=60000):
                page.click("button[type='submit']")
        except Exception:
            pass
        page.wait_for_url(re.compile(".*/dashboard.php"), timeout=30000)
        print("[PORTAL] Login OK")
    except Exception as e:
        print(f"[PORTAL] ❌ Erro no login: {e}")

def atualizar_portal(page, sku: str, preco: float):
    try:
        if is_page_closed(page):
            print(f"[PORTAL] Página fechada para SKU {sku}")
            return False
        page.fill("#filter-sku", sku)
        page.press("#filter-sku", "Enter")
        page.wait_for_selector(f"#products-table tr:has-text('{sku}')", timeout=DEFAULT_TIMEOUT)
        row = page.locator(f"#products-table tr:has-text('{sku}')").first
        row.locator("button:has(i.fas.fa-edit)").click()
        page.wait_for_selector("#edit-preco", state="visible", timeout=DEFAULT_TIMEOUT)
        txt = f"{preco:.2f}"
        page.fill("#edit-preco", txt)
        page.locator(".modal-footer button:has-text('Salvar')").click()
        page.wait_for_selector("#edit-preco", state="hidden", timeout=DEFAULT_TIMEOUT)
        print(f"[PORTAL] SKU {sku} atualizado -> {txt}")
        return True
    except Exception as e:
        print(f"[PORTAL] ❌ {sku}: {e}")
        return False

# =======================
# CDS (ERP)
# =======================
def fechar_modal_cds(page):
    try:
        page.wait_for_selector("#info-modal", state="visible", timeout=2000)
        page.locator("#btn_fechar_modal button").click()
        page.wait_for_selector("#info-modal", state="hidden", timeout=2000)
        print("[CDS] Modal de confirmação fechado")
    except:
        pass

def login_cds(page):
    for attempt in range(2):
        try:
            print(f"[CDS] Tentativa {attempt+1}: Navegando para {CDS_URL}...")
            page.goto(CDS_URL, wait_until="domcontentloaded", timeout=60000)
            print(f"[CDS] Página carregada. URL atual: {page.url}")
            
            print(f"[CDS] Preenchendo credenciais...")
            page.fill("#cdslogin", CDS_USER)
            page.fill("#cdssenha", CDS_PASS)
            
            print(f"[CDS] Clicando no botão de login...")
            try:
                with page.expect_navigation(wait_until="domcontentloaded", timeout=60000):
                    page.click("#btn-login")
            except Exception as nav_err:
                print(f"[CDS] Navegação após primeiro login: {nav_err}")
                pass
            
            print(f"[CDS] Aguardando campo de usuário...")
            try:
                page.wait_for_selector("#usuariologin", state="visible", timeout=45000)
                print(f"[CDS] Campo de usuário encontrado!")
            except Exception as wait_err:
                print(f"[CDS] Campo não apareceu imediatamente, tentando limpar modais...")
                try: 
                    page.keyboard.press("Escape")
                    print(f"[CDS] Escape pressionado")
                except: pass
                try:
                    page.evaluate("""
                      for (const sel of ['#info-modal','.modal-backdrop','.loading','.carregandoVendas']) {
                        document.querySelectorAll(sel).forEach(e=>e.remove());
                      }
                    """)
                    print(f"[CDS] Modais removidos via JS")
                except: pass
                page.wait_for_selector("#usuariologin", timeout=30000)
                print(f"[CDS] Campo de usuário encontrado após limpeza!")

            print(f"[CDS] Preenchendo dados do cliente...")
            page.fill("#usuariologin", CLIENT_USER)
            page.fill("#usuariosenha", CLIENT_PASS)
            try: 
                page.select_option("#modalidade", CLIENT_MODALIDADE_VALUE)
                print(f"[CDS] Modalidade selecionada")
            except Exception as modal_err:
                print(f"[CDS] Erro ao selecionar modalidade: {modal_err}")
                pass
            
            print(f"[CDS] Fazendo login final...")
            try:
                with page.expect_navigation(wait_until="networkidle", timeout=60000):
                    page.click("#_btn-login")
                print(f"[CDS] Navegação após login final concluída")
            except Exception as final_nav_err:
                print(f"[CDS] Navegação após login final: {final_nav_err}, aguardando networkidle...")
                page.wait_for_load_state("networkidle", timeout=45000)
            
            print("[CDS] Login OK")
            return
        except Exception as e:
            print(f"[CDS] Tentativa {attempt+1} falhou: {e}")
            import traceback
            traceback.print_exc()
            if attempt < 1:  # Só espera se não for a última tentativa
                print(f"[CDS] Aguardando 1.5s antes de retentar...")
                page.wait_for_timeout(1500)
    raise RuntimeError("CDS login falhou após 2 tentativas")

# ====== SELETORES DO DATATABLES (sempre escopados ao wrapper correto) ======
DT_WRAP            = "#table-relatorio-lista-prod_wrapper"
DT_TABLE           = f"{DT_WRAP} table#table-relatorio-lista-prod"
DT_SCROLLBODY      = f"{DT_WRAP} .dataTables_scrollBody"
DT_PROCESSING      = "#table-relatorio-lista-prod_processing"  # id único
DT_FILTER_INPUT    = f"{DT_WRAP} input[aria-controls='table-relatorio-lista-prod']"
DT_LENGTH_SELECT   = f"{DT_WRAP} select[name='table-relatorio-lista-prod_length']"
DT_INFO            = "#table-relatorio-lista-prod_info"        # id único
DT_NEXT_LI         = "#table-relatorio-lista-prod_next"        # id único
DT_NEXT_ANCHOR     = f"{DT_WRAP} {DT_NEXT_LI} a"

# ====== HELPERS DO DATATABLES ======
def cds_wait_processing_off(page, timeout_each=10000):
    try:
        page.wait_for_selector(DT_PROCESSING, state="hidden", timeout=timeout_each)
    except Exception:
        pass

def cds_wait_dt_ready(page, timeout_each=15000):
    page.wait_for_selector(DT_TABLE, state="attached", timeout=timeout_each)
    sb = page.locator(DT_SCROLLBODY).filter(has=page.locator("table#table-relatorio-lista-prod")).first
    sb.wait_for(state="visible", timeout=timeout_each)
    return sb

def cds_wait_rows(page, timeout_each=15000):
    page.wait_for_selector(f"{DT_TABLE} tbody tr", state="attached", timeout=timeout_each)

def cds_force_len_100(page):
    changed = False
    if page.locator(DT_LENGTH_SELECT).count():
        try:
            page.select_option(DT_LENGTH_SELECT, "100")
            changed = True
        except Exception:
            pass
    cds_wait_processing_off(page, 8000)
    cds_wait_rows(page, 15000)
    try:
        api_ok = page.evaluate("""
            try {
              if (window.jQuery && $.fn.DataTable) {
                var dt = $('#table-relatorio-lista-prod').DataTable();
                if (dt.page.len() !== 100) dt.page.len(100).draw(false);
                return true;
              }
              return false;
            } catch(e) { return false; }
        """)
        if api_ok: changed = True
    except Exception:
        pass
    cds_wait_processing_off(page, 8000)
    cds_wait_rows(page, 15000)
    try:
        page.wait_for_function(r"""
            () => {
                const el = document.querySelector('#table-relatorio-lista-prod_info');
                if (!el) return false;
                const t = (el.textContent || '').replace(/\s+/g,' ');
                return !/\b1 a 10\b/.test(t);
            }
        """, timeout=8000)
    except Exception:
        pass
    return changed

def cds_search_apply(page, text: str):
    if page.locator(DT_FILTER_INPUT).count():
        inp = page.locator(DT_FILTER_INPUT).first
        inp.click()
        try:
            inp.fill("")
        except Exception:
            for _ in range(40):
                try: inp.press("Backspace")
                except: break
        inp.type(str(text), delay=15)
        try: inp.press("Enter")
        except Exception: pass

    cds_wait_processing_off(page, 8000)
    cds_wait_dt_ready(page, 15000)
    cds_force_len_100(page)
    try:
        page.evaluate("""
            try {
              if (window.jQuery && $.fn.DataTable) {
                var dt = $('#table-relatorio-lista-prod').DataTable();
                var q = %s;
                if (dt.search() !== q) { dt.search(q).draw(false); }
              }
            } catch(e) {}
        """ % json.dumps(str(text)))
    except Exception:
        pass

    cds_wait_processing_off(page, 8000)
    cds_wait_dt_ready(page, 15000)
    cds_force_len_100(page)

def cds_clear_search(page):
    cds_search_apply(page, "")

def cds_find_in_current_page_by_hidden_input(page, sku: str):
    row = page.locator(
        f"{DT_TABLE} tbody tr"
    ).filter(
        has=page.locator(f"input[id^='grid_codigo_prod_'][value='{sku}']")
    ).first
    return row if row.count() > 0 else None

def cds_find_in_current_page_by_codigo_base(page, sku: str):
    cell = page.locator(
        f"{DT_TABLE} tbody tr td:nth-child(2)"
    ).filter(
        has_text=re.compile(rf"^{re.escape(str(sku))}$")
    ).first
    if cell.count() == 0:
        return None
    return cell.locator("xpath=ancestor::tr[1]").first

def cds_jump_to_page_of_sku_via_api(page, sku: str) -> bool:
    try:
        ok = page.evaluate("""
            try {
              if (!(window.jQuery && $.fn.DataTable)) return false;
              var dt = $('#table-relatorio-lista-prod').DataTable();
              if (dt.page.len() !== 100) dt.page.len(100);
              var perPage = dt.page.len();
              var target = %s + "";
              function textOf(html) {
                var d = document.createElement('div'); d.innerHTML = html;
                return (d.textContent || d.innerText || '').trim();
              }
              var data = dt.column(1).data().toArray();
              var idx = -1;
              for (var i=0;i<data.length;i++){
                if (textOf(data[i]) === target) { idx = i; break; }
              }
              if (idx < 0) return false;
              var pageNum = Math.floor(idx / perPage);
              dt.page(pageNum).draw(false);
              return true;
            } catch(e){ return false; }
        """ % json.dumps(str(sku)))
    except Exception:
        ok = False

    cds_wait_processing_off(page, 8000)
    cds_wait_dt_ready(page, 15000)
    cds_force_len_100(page)
    return bool(ok)

def cds_consultar(page):
    try:
        page.wait_for_selector("#tabela", timeout=10000)
        try:
            page.select_option("#tabela", label=re.compile(r"TODAS AS TABELAS", re.I))
        except Exception:
            try: page.select_option("#tabela", value="")
            except Exception: pass
    except Exception:
        pass

    if page.locator("#btn-consultar-lista-produtos").count():
        try:
            page.click("#btn-consultar-lista-produtos")
        except Exception:
            try: page.evaluate("document.querySelector('#btn-consultar-lista-produtos')?.click()")
            except Exception: pass

    try:
        page.wait_for_selector(DT_PROCESSING, state="visible", timeout=5000)
    except Exception:
        pass
    cds_wait_processing_off(page, 20000)
    cds_wait_dt_ready(page, 20000)
    cds_wait_rows(page, 20000)
    cds_force_len_100(page)

def cds_find_row(page, sku: str, timeout_each=20000):
    cds_consultar(page)

    cds_search_apply(page, sku)
    row = cds_find_in_current_page_by_hidden_input(page, sku) or cds_find_in_current_page_by_codigo_base(page, sku)
    if row:
        return row

    cds_clear_search(page)

    if cds_jump_to_page_of_sku_via_api(page, sku):
        row = cds_find_in_current_page_by_hidden_input(page, sku) or cds_find_in_current_page_by_codigo_base(page, sku)
        if row:
            print(f"[CDS] SKU {sku} encontrado via DataTables API (salto).")
            return row

    def next_is_disabled():
        if not page.locator(DT_NEXT_LI).count():
            return True
        cls = (page.locator(DT_NEXT_LI).first.get_attribute("class") or "").lower()
        return "disabled" in cls

    visited = 0
    while True:
        visited += 1
        cds_wait_processing_off(page, 8000)
        cds_wait_dt_ready(page, 15000)
        cds_force_len_100(page)
        cds_wait_rows(page, 15000)

        row = cds_find_in_current_page_by_hidden_input(page, sku) or cds_find_in_current_page_by_codigo_base(page, sku)
        if row:
            if visited > 1:
                print(f"[CDS] SKU {sku} encontrado paginando (página {visited}).")
            return row

        if next_is_disabled():
            break

        try:
            page.locator(DT_NEXT_ANCHOR).first.click()
        except Exception:
            try:
                page.locator("#table-relatorio-lista-prod_paginate li.next:not(.disabled) a").first.click()
            except Exception:
                break

        page.wait_for_timeout(120)
        cds_wait_processing_off(page, 8000)

    return None

def atualizar_cds(page, sku: str, preco: float):
    try:
        if is_page_closed(page):
            print(f"[CDS] Página fechada para SKU {sku}")
            return False
        page.goto(CDS_URL + "relatorio-dos-produtos", wait_until="domcontentloaded", timeout=60000)

        cds_consultar(page)

        row = cds_find_row(page, sku, timeout_each=DEFAULT_TIMEOUT)
        if row is None:
            print(f"[CDS] SKU {sku} não encontrado após varrer as páginas.")
            return False

        try:
            row.scroll_into_view_if_needed(timeout=3000)
        except Exception:
            pass

        btn_edit = row.locator("button.btn_edita_prod").first
        btn_edit.click()

        page.wait_for_selector("#vendaPrc", state="visible", timeout=DEFAULT_TIMEOUT)

        val = as_br_price(preco)
        print(f"[CDS] Preenchendo preço: {val} (valor original: {preco})")
        
        # Verifica o preço atual antes de preencher
        for selector in ["#vendaPrc", "#vendaPrcA", "#vendaPrcC"]:
            if page.locator(selector).count():
                try:
                    valor_atual = page.locator(selector).first.input_value()
                    if valor_atual:
                        print(f"[CDS] Preço atual no campo {selector}: {valor_atual}")
                except:
                    pass
        
        for selector in ["#vendaPrc", "#vendaPrcA", "#vendaPrcC"]:
            if page.locator(selector).count():
                page.fill(selector, val)

        # Aguarda um pouco para o preço ser processado
        page.wait_for_timeout(500)
        
        # Tenta usar o ID específico do botão de salvar produto
        btn_salvar = page.locator("#btn_salvar_produto")
        if btn_salvar.count() > 0:
            # Garante que o botão está visível e habilitado
            btn_salvar.wait_for(state="visible", timeout=10000)
            try:
                # Rola até o botão se necessário
                btn_salvar.scroll_into_view_if_needed(timeout=3000)
            except:
                pass
            btn_salvar.click()
            cds_wait_processing_off(page, 6000)
            fechar_modal_cds(page)
        else:
            # Fallback para outros botões de salvar
            for sel in ["button:has-text('Atualizar')", ".modal-footer button:has-text('Salvar')"]:
                if page.locator(sel).count() > 0:
                    page.locator(sel).first.wait_for(state="visible", timeout=10000)
                    page.locator(sel).first.click()
                    cds_wait_processing_off(page, 6000)
                    fechar_modal_cds(page)
                    break

        print(f"[CDS] SKU {sku} atualizado -> {val}")
        return True

    except Exception as e:
        print(f"[CDS] ❌ {sku}: {e}")
        return False

# =======================
# WP-Admin
# =======================
def wp_login(page):
    login_url = f"{WP_BASE_URL}/wp-login.php?redirect_to={urllib.parse.quote(WP_BASE_URL + '/wp-admin/')}"
    page.goto(login_url, wait_until="domcontentloaded", timeout=60000)
    page.fill('input[name="log"]', WP_USER)
    page.fill('input[name="pwd"]', WP_PASS)
    try:
        with page.expect_navigation(wait_until="domcontentloaded", timeout=60000):
            page.click('input[name="wp-submit"]')
    except Exception:
        pass
    try:
        page.wait_for_url(re.compile(r".*/wp-admin/.*"), timeout=30000)
    except Exception:
        page.wait_for_selector("#wpadminbar, body.wp-admin", timeout=30000)
    print("[WP] Login OK")

def atualizar_woo(page, sku: str, preco: float):
    try:
        if is_page_closed(page):
            print(f"[WP] Página fechada para SKU {sku}")
            return None
        page.goto(f"{WP_BASE_URL}/wp-admin/edit.php?post_type=product", wait_until="domcontentloaded", timeout=60000)
        page.fill("#post-search-input", sku)
        page.click("#search-submit")
        page.wait_for_selector("table.wp-list-table tbody", timeout=DEFAULT_TIMEOUT)

        has_row = page.locator("table.wp-list-table tbody tr .row-title").count() > 0
        no_items = page.locator("table.wp-list-table tbody tr.no-items").count() > 0

        if (not has_row) or no_items:
            if wc:
                try:
                    r = wc.get("products", params={"sku": sku})
                    data = r.json()
                    if not data:
                        print(f"[WP] SKU {sku} não encontrado — pulando Woo")
                        return None
                except Exception:
                    print(f"[WP][REST] Erro ao consultar SKU {sku} (não foi possível confirmar inexistência).")
                    return False
            else:
                print(f"[WP] SKU {sku} não encontrado — pulando Woo")
                return None

        page.locator("table.wp-list-table tbody tr .row-title").first.click()
        page.wait_for_selector("#_regular_price, input[name='_regular_price']", timeout=DEFAULT_TIMEOUT)

        txt = f"{preco:.2f}".replace(".", ",")
        if page.locator("#_regular_price").count():
            page.fill("#_regular_price", txt)
        else:
            page.fill("input[name='_regular_price']", txt)

        if page.locator("#publish").count():
            page.click("#publish")
        elif page.locator("button.editor-post-publish-button").count():
            page.click("button.editor-post-publish-button")

        try:
            page.wait_for_selector(".updated.notice-success, .notice-success, #message.updated", timeout=10000)
        except Exception:
            page.wait_for_load_state("networkidle", timeout=8000)

        print(f"[WP] SKU {sku} -> {txt}")
        return True

    except Exception as e:
        if wc:
            try:
                r = wc.get("products", params={"sku": sku})
                data = r.json()
                if not data:
                    print(f"[WP][REST] SKU {sku} não encontrado — pulando Woo")
                    return None
                pid = data[0].get("id")
                wc.put(f"products/{pid}", {"regular_price": f"{preco:.2f}"})
                print(f"[WP][REST] SKU {sku} -> {preco:.2f}")
                return True
            except Exception as e2:
                print(f"[WP][REST] Falha: {e2}")
        print(f"[WP] ❌ {sku}: {e}")
        return False

# =======================
# API Produtos
# =======================
def carregar_produtos():
    url = "https://mlovi.com.br/sistemahortigold/endpoints/test_products.php"
    try:
        resp = requests.get(url, timeout=15)
        resp.raise_for_status()
        data = resp.json()
        if not data.get("success"):
            print("[API] ❌ Resposta inválida:", data)
            return []
        products = []
        for prod in data.get("products", []):
            sku = (prod.get("sku") or "").strip()
            nome = (prod.get("nome") or "").strip()
            incremento = float(prod.get("incremento_preco") or 0.0)
            if sku and nome:
                products.append({"sku": sku, "nome": nome, "incremento": incremento})
        print(f"[API] {len(products)} produtos carregados da API")
        return products
    except Exception as e:
        print(f"[API] ⚠️ Erro ao carregar produtos: {e}")
        return []

# =======================
# TENDA — Busca por URL e preço unitário
# =======================
SEARCH_INPUT       = "#searchbarComponent"
RESULTS_CONTAINER  = ".MosaicCardContainer, .box-group.mosaic-container"
CARD_ANCHOR        = "a.showcase-card-content"
CARD_TITLE_SEL     = "h3.TitleCardComponent"
UNIT_PRICE_SEL     = ".SimplePriceComponent"

MODAL_CONTAINER  = ".ShippingModalContainer.medium"
MODAL_ID         = "#modal-shipping"
MODAL_VISIBLE_Q  = f"{MODAL_ID}.show, {MODAL_CONTAINER} .ModalDefault.show"
CEP_INPUT_SEL    = "#shipping-cep"
CLOSE_BUTTONS    = "img.svgIcon.svg-ico_close_with_circle, button[title*='Fechar'], button[aria-label*='Fechar']"
BLACK_BLOCK      = ".black-block"

def build_cep_observer_js(cep: str) -> str:
    digits = "".join(ch for ch in cep if ch and ch.isdigit())
    return f"""
(() => {{
  if (window.__cepObserverInstalled) return;
  window.__cepObserverInstalled = true;
  const CEP_DIGITS = "{digits}";
  const INPUT_SEL  = "#shipping-cep";
  const CLOSE_SEL  = "img.svgIcon.svg-ico_close_with_circle, button[title*='Fechar'], button[aria-label*='Fechar']";
  let lastApply = 0;
  function isVisible(el) {{ return !!(el && el.offsetParent !== null); }}
  function pressEnter(el) {{
    try {{
      el.dispatchEvent(new KeyboardEvent('keydown', {{key:'Enter', bubbles:true}}));
      el.dispatchEvent(new KeyboardEvent('keyup',   {{key:'Enter', bubbles:true}}));
    }} catch (e) {{}}
  }}
  async function typeSlow(el, text, delay=25) {{
    el.value = ""; el.dispatchEvent(new Event('input', {{bubbles:true}}));
    for (let i=0;i<text.length;i++) {{
      el.value += text[i]; el.dispatchEvent(new Event('input', {{bubbles:true}}));
      await new Promise(r => setTimeout(r, delay));
    }}
    el.dispatchEvent(new Event('change', {{bubbles:true}}));
  }}
  function closeModal() {{
    try {{
      // Tenta fechar via jQuery/Bootstrap se disponível
      if (window.jQuery) {{
        try {{
          jQuery('#modal-shipping').modal('hide');
          jQuery('.modal-backdrop').remove();
        }} catch(e) {{}}
      }}
      
      // Tenta clicar no botão de fechar
      const closeBtn = document.querySelector(CLOSE_SEL);
      if (closeBtn) {{
        closeBtn.click();
        return true;
      }}
      
      // Remove elementos do modal
      const selectors = ['.black-block', '#modal-shipping', '.ModalDefault.show', '.ShippingModalContainer.medium', '.modal-backdrop'];
      selectors.forEach(sel => {{
        document.querySelectorAll(sel).forEach(el => {{
          try {{
            el.style.display = 'none';
            el.remove();
          }} catch(e) {{}}
        }});
      }});
      
      // Remove classes do body
      document.body.classList.remove('modal-open');
      document.body.style.overflow = 'auto';
      document.body.style.paddingRight = '';
      return true;
    }} catch(e) {{
      return false;
    }}
  }}
  
  async function applyCepIfNeeded() {{
    const now = Date.now(); if (now - lastApply < 1200) return;
    const inp = document.querySelector(INPUT_SEL); if (!isVisible(inp)) return;
    const current = (inp.value || "").replace(/\\D/g, "");
    if (current === CEP_DIGITS) {{
      setTimeout(() => {{ closeModal(); }}, 1000);
      lastApply = now;
      return;
    }}
    try {{
      inp.focus(); await typeSlow(inp, CEP_DIGITS, 28); pressEnter(inp);
      setTimeout(() => {{ closeModal(); }}, 1000);
      lastApply = Date.now();
    }} catch (e) {{
      try {{
        inp.value = CEP_DIGITS;
        inp.dispatchEvent(new Event('input', {{bubbles:true}}));
        inp.dispatchEvent(new Event('change', {{bubbles:true}}));
        pressEnter(inp);
        setTimeout(() => {{ closeModal(); }}, 1000);
        lastApply = Date.now();
      }} catch (e2) {{}}
    }}
  }}
  applyCepIfNeeded();
  new MutationObserver(() => applyCepIfNeeded())
    .observe(document.documentElement, {{childList:true, subtree:true, attributes:true, attributeFilter:['style','class','open','aria-hidden']}});
  window.__applyCepIfNeeded = applyCepIfNeeded;
}})();
"""

def nuke_overlays(page):
    """Remove modais e overlays da Tenda"""
    try:
        # Primeiro tenta clicar no botão de fechar se existir
        close_btn = page.locator("img.svgIcon.svg-ico_close_with_circle").first
        if close_btn.count() > 0:
            try:
                close_btn.click(timeout=2000)
                page.wait_for_timeout(300)
            except:
                pass
        
        # Depois remove os elementos via JS
        page.evaluate("""
          (function() {
            // Tenta fechar o modal via eventos do Bootstrap/jQuery se disponível
            try {
              if (window.jQuery) {
                jQuery('#modal-shipping').modal('hide');
                jQuery('.modal-backdrop').remove();
              }
            } catch(e) {}
            
            // Remove elementos do modal
            const selectors = [
              '.black-block',
              '#modal-shipping',
              '.ModalDefault.show',
              '.ShippingModalContainer.medium',
              '.modal-backdrop'
            ];
            
            selectors.forEach(sel => {
              document.querySelectorAll(sel).forEach(el => {
                try {
                  el.style.display = 'none';
                  el.remove();
                } catch(e) {}
              });
            });
            
            // Remove classes do body
            document.body.classList.remove('modal-open');
            document.body.style.overflow = 'auto';
            document.body.style.paddingRight = '';
          })();
        """)
        print("[Tenda] Overlay removido via JS")
    except Exception as e:
        print(f"[Tenda] Erro ao remover overlay: {e}")
        pass

def ensure_cep(page, cep):
    """Garante que o CEP está preenchido e fecha o modal"""
    try:
        # Verifica se o modal está visível
        modal_visible = False
        try:
            modal_visible = page.locator(MODAL_VISIBLE_Q).first.is_visible(timeout=2000)
        except:
            try:
                modal_visible = page.locator("#modal-shipping.show").count() > 0
            except:
                pass
        
        if modal_visible or page.locator(CEP_INPUT_SEL).is_visible(timeout=2000):
            try:
                # Preenche o CEP se o campo estiver visível
                cep_input = page.locator(CEP_INPUT_SEL).first
                if cep_input.is_visible(timeout=2000):
                    cep_input.fill("")
                    cep_digits = re.sub(r"\D", "", cep)
                    for ch in cep_digits:
                        cep_input.type(ch, delay=20)
                    cep_input.press("Enter")
                    page.wait_for_timeout(500)  # Aguarda processamento
            except Exception as e:
                print(f"[Tenda] Erro ao preencher CEP: {e}")
            
            # Fecha o modal
            nuke_overlays(page)
            
            # Verifica se o modal foi fechado
            page.wait_for_timeout(300)
            try:
                if page.locator("#modal-shipping.show").count() > 0:
                    print("[Tenda] Modal ainda visível, tentando fechar novamente...")
                    nuke_overlays(page)
            except:
                pass
    except Exception as e:
        print(f"[Tenda] Erro em ensure_cep: {e}")
        # Tenta fechar mesmo assim
        try:
            nuke_overlays(page)
        except:
            pass

def tenda_do_search(page, query: str):
    q_enc = urllib.parse.quote(query)
    page.goto(f"{TENDA_URL}/busca?q={q_enc}", wait_until="domcontentloaded", timeout=60000)

def tenda_has_zero_results(page) -> bool:
    try:
        zero_counter = page.locator(".SearchContainer h1.area-result strong").first
        if zero_counter.count():
            txt = (zero_counter.inner_text() or "").strip()
            if txt == "0":
                return True
        if page.locator(".mosaic-container.notFound").count() > 0:
            return True
        if page.locator(".EmptyAreaComponent .title", has_text="Não existem produtos").count() > 0:
            return True
    except Exception:
        pass
    return False

def buscar_preco_tenda(page, query: str):
    try:
        if is_page_closed(page):
            print("[Tenda] Página fechada")
            return None
        if TENDA_URL not in page.url:
            page.goto(TENDA_URL, wait_until="domcontentloaded", timeout=60000)
            page.wait_for_timeout(500)
        if USE_CEP:
            ensure_cep(page, CEP_VALOR)
            nuke_overlays(page)

        tenda_do_search(page, query)
        if USE_CEP:
            ensure_cep(page, CEP_VALOR)
            nuke_overlays(page)

        page.wait_for_function("""
            () => {
              const hasCards = document.querySelectorAll("a.showcase-card-content").length > 0;
              const notFound = document.querySelector(".mosaic-container.notFound, .EmptyAreaComponent") != null;
              const counter = document.querySelector(".SearchContainer h1.area-result strong");
              const zero = counter && (counter.textContent||'').trim() === '0';
              return hasCards || notFound || zero;
            }
        """, timeout=DEFAULT_TIMEOUT)

        if tenda_has_zero_results(page):
            termo = query.strip()
            print(f"[Tenda] 0 resultados para \"{termo}\" — pulando SKU.")
            return None

        try:
            page.wait_for_selector(CARD_ANCHOR, state="attached", timeout=DEFAULT_TIMEOUT)
        except Exception:
            page.wait_for_load_state("networkidle", timeout=DEFAULT_TIMEOUT)

        cards = page.locator(CARD_ANCHOR)
        count = cards.count()
        if count == 0:
            print("[Tenda] Nenhum card encontrado (não é tela de 0 resultados, mas não há cards).")
            return None

        q_tokens = [t for t in re.findall(r"[a-z0-9]+", query.lower()) if len(t) > 1]
        best_score, best_price = -1.0, None

        lim = min(12, count)
        for i in range(lim):
            card = cards.nth(i)
            try:
                name = card.locator(CARD_TITLE_SEL).first.inner_text().strip().lower()
            except Exception:
                name = ""
            try:
                unit_raw = card.locator(UNIT_PRICE_SEL).first.inner_text().strip()
                unit_price = clean_price(unit_raw)
            except Exception:
                unit_price = None

            if unit_price is None:
                continue

            score = (sum(1 for t in q_tokens if t in name) / max(1, len(q_tokens))) if name else 0.0
            if score > best_score:
                best_score, best_price = score, unit_price

            if i == 0 and best_price is not None and score >= 0.6:
                break

        if best_price is not None:
            print(f"[Tenda][Resultados] preço unitário = {best_price}")
            return best_price

        for i in range(lim):
            card = cards.nth(i)
            try:
                unit_raw = card.locator(UNIT_PRICE_SEL).first.inner_text().strip()
                unit_price = clean_price(unit_raw)
                if unit_price is not None:
                    print(f"[Tenda][Resultados][fallback] preço unitário = {unit_price}")
                    return unit_price
            except Exception:
                continue

        print("[Tenda] Não foi possível extrair preço unitário.")
        return None

    except Exception as e:
        print(f"[Tenda] ❌ Erro na busca: {e}")
        return None

# =======================
# MAIN (com batching)
# =======================
def main():
    log_file = get_log_filename()
    print(f"[LOG] Registrando no arquivo: {log_file}")
    
    produtos = carregar_produtos()
    if not produtos:
        print("[Init] Nenhum produto carregado da API")
        return

    print(f"[Init] {len(produtos)} SKUs para processar (lotes de {BATCH_SIZE})")
    
    start_global = time.time()
    ok = err = miss = 0
    
    # Inicia o Playwright Manager uma única vez
    with sync_playwright() as pw:
        # Loop pelos lotes
        for batch_idx, batch in enumerate(chunked(produtos, BATCH_SIZE), start=1):
            print(f"\n====== Lote {batch_idx} ({len(batch)} itens) ======")
            
            browser = None
            ctx = None
            p_tenda = p_cds = p_wp = p_portal = None
            
            try:
                # --- [FIX CRÍTICO] ---
                # Lançamos o navegador AQUI, para cada lote.
                # Se o lote anterior crashou o browser, este nasce novo.
                print(f"[Lote {batch_idx}] Criando navegador (modo headless, sem UI)...")
                launch_args = ["--lang=pt-BR", "--disable-blink-features=AutomationControlled"]
                
                # flags só para Linux (VPS/Docker)
                if sys.platform.startswith("linux"):
                    launch_args += ["--no-sandbox", "--disable-dev-shm-usage"]
                
                print(f"[Lote {batch_idx}] Argumentos: {launch_args}")
                browser = pw.chromium.launch(
                    headless=HEADLESS,  # Usa a variável HEADLESS da configuração
                    slow_mo=SLOW_MO_MS,
                    devtools=False,  # Sempre sem DevTools
                    args=launch_args,
                )
                print(f"[Lote {batch_idx}] ✅ Navegador criado com sucesso")
                
                # Cria contexto e páginas
                print(f"[Lote {batch_idx}] Criando contexto...")
                ctx = make_context_only(browser)
                print(f"[Lote {batch_idx}] ✅ Contexto criado")
                
                print(f"[Lote {batch_idx}] Iniciando logins...")
                p_tenda, p_cds, p_wp, p_portal = open_and_login_all(ctx)
                print(f"[Lote {batch_idx}] ✅ Logins concluídos")
                
                t_lote = time.time()
                
                # Processa os produtos do lote
                for prod in batch:
                    sku = prod["sku"]
                    query = prod["nome"]
                    incremento = float(prod["incremento"])
                    
                    print(f"\n=== {sku} | {query} ===")
                    t_prod = time.time()
                    
                    # 1. Busca Tenda
                    try:
                        preco_base = buscar_preco_tenda(p_tenda, query)
                    except Exception as e:
                        print(f"[Tenda] Erro busca: {e}")
                        preco_base = None
                        # Tenta recuperar a página da Tenda se ela morreu
                        if is_page_closed(p_tenda):
                            print("[Tenda] Página morreu, tentando recriar...")
                            try: 
                                p_tenda = ctx.new_page()
                                p_tenda.goto(TENDA_URL)
                            except: pass

                    if not preco_base:
                        miss += 1
                        log_produto(sku, query, None, "IGNORADO", log_file)
                        continue

                    # 2. Atualizações
                    print(f"[Cálculo] Preço base (Tenda)={preco_base} | Incremento={incremento}%")
                    
                    # O sistema CDS parece aplicar o incremento automaticamente ao salvar.
                    # Baseado nos dados: Preço Base=7.43, Incremento=33%, Preço Final=9.88
                    # Isso sugere que o CDS aplica incremento sobre o valor enviado.
                    # Portanto, precisamos enviar o preço que, após o incremento do CDS, resulte no valor correto.
                    # Se o esperado é 9.88 e o incremento é 33%, então:
                    # preco_enviado × 1.33 = 9.88 → preco_enviado = 9.88 / 1.33 = 7.43
                    # Mas o preço base da Tenda é 5.59, então precisamos aplicar o incremento primeiro.
                    preco_com_incremento = round(preco_base * (1 + incremento / 100.0), 2)
                    preco_final = preco_com_incremento
                    print(f"[Cálculo] Preço com incremento aplicado={preco_final} (Base {preco_base} × {1 + incremento/100.0:.4f})")
                    print(f"[Cálculo] ATENÇÃO: Se o CDS aplicar incremento novamente, o resultado será {preco_final * (1 + incremento/100.0):.2f}")

                    cds_ok = atualizar_cds(p_cds, sku, preco_final) if p_cds else False
                    woo_ok = atualizar_woo(p_wp, sku, preco_final) if p_wp else None
                    portal_ok = atualizar_portal(p_portal, sku, preco_final) if p_portal else False
                    
                    status = "OK" if woo_ok is True else "OK_SEM_WOO"
                    if cds_ok and portal_ok and (woo_ok is not False):
                        ok += 1
                        log_produto(sku, query, preco_final, status, log_file)
                    else:
                        err += 1
                        log_produto(sku, query, preco_final, "ERRO_PARCIAL", log_file)
                    
                    log_step(f"Produto {sku} fim", t_prod)

                log_step(f"Lote {batch_idx} concluído", t_lote)

            except Exception as e:
                print(f"[Lote {batch_idx}] ❌ ERRO FATAL NO LOTE: {e}")
                err += len(batch)
                # O Python vai para o 'finally', fecha tudo e o próximo lote abre um browser novo
            
            finally:
                # Fecha páginas
                for p in [p_tenda, p_cds, p_wp, p_portal]:
                    try: 
                        if p: p.close()
                    except: pass
                # Fecha contexto
                try: 
                    if ctx: ctx.close()
                except: pass
                # Fecha Browser
                try:
                    if browser: 
                        browser.close()
                        print("[Browser] Fechado para reciclagem.")
                except: pass
                
                # Pequena pausa para o SO liberar as portas
                time.sleep(2)

    log_step("Processo completo", start_global)
    total = len(produtos)
    print(f"\n[Resumo Final] OK={ok} | Falhas={err} | Ignorados={miss} | Total={total}")
    print(f"[Fim] {datetime.datetime.utcnow().isoformat()}Z")

if __name__ == "__main__":
    main()
