import streamlit as st
import psycopg2
from jinja2 import Environment, FileSystemLoader
from datetime import datetime, timezone, timedelta

# 레이아웃 간소화 (가장 먼저 선언)
st.set_page_config(page_title="통합 ETL DAG 생성기", layout="wide")

# ==========================================
# 🗄️ 타임존 및 DB 연동 유틸리티 (NeonDB)
# ==========================================
def get_kst_now():
    """현재 한국 시간(KST)을 반환하는 함수 (DB timestamp 적재용)"""
    KST = timezone(timedelta(hours=9))
    return datetime.now(KST).replace(tzinfo=None)

def get_db_connection():
    """NeonDB 연결 객체 생성"""
    return psycopg2.connect(
        host=st.secrets["PG_HOST"],
        port=st.secrets["PG_PORT"],
        database=st.secrets["PG_DATABASE"],
        user=st.secrets["PG_USER"],
        password=st.secrets["PG_PASSWORD"]
    )

def verify_user(user_id, password):
    """public.users 테이블 조회 및 인증"""
    try:
        conn = get_db_connection()
        cur = conn.cursor()
        
        query = "SELECT name FROM public.users WHERE id = %s AND pswd = %s"
        cur.execute(query, (user_id, password))
        user = cur.fetchone()
        
        cur.close()
        conn.close()
        return user[0] if user else None
    except Exception as e:
        st.error(f"DB 인증 중 오류 발생: {e}")
        return None

def change_password(user_id, current_pw, new_pw):
    """현재 비밀번호 확인 후 새 비밀번호로 업데이트 (KST 적용)"""
    try:
        conn = get_db_connection()
        cur = conn.cursor()
        
        # 1. ID와 현재 비밀번호가 맞는지 검증
        cur.execute("SELECT name FROM public.users WHERE id = %s AND pswd = %s", (user_id, current_pw))
        user = cur.fetchone()
        
        if not user:
            return False, "입력하신 ID 또는 현재 비밀번호가 일치하지 않습니다.", None
            
        user_name = user[0]
        
        # 2. 비밀번호 및 업데이트 일시(KST) 갱신
        update_query = """
            UPDATE public.users 
            SET pswd = %s, update_dttm = %s 
            WHERE id = %s
        """
        cur.execute(update_query, (new_pw, get_kst_now(), user_id))
        conn.commit()
        
        cur.close()
        conn.close()
        return True, "✨ 비밀번호가 성공적으로 변경되었습니다. 변경된 비밀번호로 로그인해 주세요.", user_name
    except Exception as e:
        return False, f"비밀번호 변경 중 DB 오류 발생: {e}", None

def insert_log(user_id, user_name, event_name, script=None):
    """airflow_generator_log 테이블에 사용자 활동 로깅 (KST 적용)"""
    try:
        conn = get_db_connection()
        cur = conn.cursor()
        query = """
            INSERT INTO public.airflow_generator_log (id, name, event_name, event_dttm, script)
            VALUES (%s, %s, %s, %s, %s)
        """
        cur.execute(query, (user_id, user_name, event_name, get_kst_now(), script))
        conn.commit()
        cur.close()
        conn.close()
    except Exception as e:
        st.error(f"로그 적재 중 오류 발생: {e}")

# ==========================================
# 🔐 DB 기반 로그인 & 비밀번호 변경 화면 (Pre-Login)
# ==========================================
def auth_screen():
    if "authenticated" not in st.session_state:
        st.session_state["authenticated"] = False
    
    # 화면 전환을 위한 상태값 (login 또는 change_pw)
    if "auth_view_mode" not in st.session_state:
        st.session_state["auth_view_mode"] = "login"

    if not st.session_state["authenticated"]:
        
        # --- [모드 1] 로그인 화면 ---
        if st.session_state["auth_view_mode"] == "login":
            st.markdown("## 🔒 LG ONED 플랫폼 로그인")
            st.markdown("통합 ETL DAG 생성기에 접근하려면 DB 계정으로 로그인해 주세요.")
            
            with st.form("login_form"):
                user_id = st.text_input("사용자 ID")
                password = st.text_input("비밀번호", type="password")
                login_btn = st.form_submit_button("로그인", type="primary")

                if login_btn:
                    user_name = verify_user(user_id, password)
                    if user_name:
                        st.session_state["authenticated"] = True
                        st.session_state["user_id"] = user_id
                        st.session_state["user_name"] = user_name
                        insert_log(user_id, user_name, "LOGIN_SUCCESS")
                        st.rerun()
                    else:
                        st.error("😕 ID 또는 비밀번호가 올바르지 않습니다.")
            
            st.write("")
            if st.button("비밀번호를 변경하시겠습니까?"):
                st.session_state["auth_view_mode"] = "change_pw"
                st.rerun()

        # --- [모드 2] 비밀번호 변경 화면 ---
        elif st.session_state["auth_view_mode"] == "change_pw":
            st.markdown("## 🔑 비밀번호 변경")
            st.markdown("현재 사용 중인 ID와 비밀번호를 인증한 후 새 비밀번호로 변경할 수 있습니다.")
            
            with st.form("pwd_change_form", clear_on_submit=True):
                c_user_id = st.text_input("사용자 ID")
                curr_pw = st.text_input("현재 비밀번호", type="password")
                st.markdown("---")
                new_pw = st.text_input("새 비밀번호 (4자리 이상)", type="password")
                new_pw_check = st.text_input("새 비밀번호 확인", type="password")
                    
                btn_change_pwd = st.form_submit_button("비밀번호 변경하기", type="primary")
                
                if btn_change_pwd:
                    if not c_user_id or not curr_pw or not new_pw or not new_pw_check:
                        st.warning("모든 필드를 입력해 주세요.")
                    elif new_pw != new_pw_check:
                        st.error("새 비밀번호가 일치하지 않습니다.")
                    elif len(new_pw) < 4:
                        st.warning("새 비밀번호는 4자리 이상으로 설정해 주세요.")
                    elif curr_pw == new_pw:
                        st.warning("새 비밀번호는 현재 비밀번호와 다르게 설정해 주세요.")
                    else:
                        success, msg, user_name = change_password(c_user_id, curr_pw, new_pw)
                        if success:
                            st.success(msg)
                            insert_log(c_user_id, user_name, "PASSWORD_CHANGE")
                        else:
                            st.error(msg)

            st.write("")
            if st.button("⬅️ 로그인 화면으로 돌아가기"):
                st.session_state["auth_view_mode"] = "login"
                st.rerun()
                
        return False
    return True

# 인증되지 않았으면 여기서 앱 실행 중지
if not auth_screen():
    st.stop()

# ==========================================
# 메인 플랫폼 화면 시작
# ==========================================
col_title, col_logout = st.columns([8, 1])
with col_title:
    st.markdown(f"### 🚀 OneData 통합 ETL DAG 생성 플랫폼 (접속자: {st.session_state['user_name']})")
with col_logout:
    if st.button("로그아웃"):
        insert_log(st.session_state["user_id"], st.session_state["user_name"], "LOGOUT")
        st.session_state["authenticated"] = False
        st.rerun()

st.markdown("---")

# ==========================================
# 1. 기본 정보 및 프로젝트/유형 선택
# ==========================================
st.markdown("#### 1. 프로젝트, 기본 정보 및 생성 유형")
col0, col1, col2, col3, col4, col5 = st.columns([0.9, 1.2, 0.8, 1.2, 1.4, 1.5])
with col0:
    project_name = st.selectbox("🏢 프로젝트", ["LG ONED"])
with col1:
    dag_id = st.text_input("DAG ID", placeholder="ONED_MIG_01")
with col2:
    author = st.text_input("Owner", value=st.session_state["user_name"]) # 로그인한 유저명 자동 맵핑
with col3:
    email = st.text_input("Email", placeholder="user@lgepartner.com")
with col4:
    description = st.text_input("Description", placeholder="DAG 설명")
with col5:
    dag_types = [
        "표준 ETL (단일 쿼리 적재)", 
        "마스터 DAG (Sub-DAG 호출)", 
        "프로시저 호출 (Stored Procedure)", 
        "반복문 적재 (Loop ETL)", 
        "커스텀 라이브러리 실행 (Python)",
        "태블로원본 추출 (Tableau)"
    ]
    dag_type = st.selectbox("🔥 DAG 구조 (유형)", dag_types)

st.markdown("---")

# 공통 변수 초기화
source_db, target_db, target_table, execute_query = "None", "None", "", ""
source_conn, target_conn, partition_column = "None", "None", ""
is_large_data, chunk_size = False, 0
sub_dag_list, poke_interval = "", 60
loop_variables, proc_name = "", ""
lib_module, lib_func = "", ""
target_datasource = ""

# ==========================================
# 2. [동적 UI] DAG 유형별 상세 로직 설정
# ==========================================
st.markdown(f"#### 2. 상세 로직 설정 : {dag_type}")

if dag_type == "표준 ETL (단일 쿼리 적재)":
    d_col1, d_col2, d_col3, d_col4 = st.columns([1, 1, 1.2, 1.2])
    with d_col1:
        source_db = st.selectbox("Source DB", ["BigQuery"])
        target_db = st.selectbox("Target DB", ["BigQuery", "PostgreSQL"])
    with d_col2:
        source_conn = st.text_input("Source Conn", value="oned-vertex")
        target_conn = st.text_input("Target Conn", value="oned-vertex" if target_db == "BigQuery" else "oned-opera-ia")
    with d_col3:
        target_table = st.text_input("Target Table", placeholder="oned_ia.table_name")
        partition_column = st.text_input("파티션 컬럼(삭제용)", value="BASE_DATE")
    with d_col4:
        is_large_data = st.checkbox("대용량 모드 (Chunk)", value=True)
        chunk_size = st.number_input("Chunk Size", min_value=10000, value=200000, step=50000, disabled=not is_large_data)
    execute_query = st.text_area("추출 쿼리 (SELECT)", height=150, placeholder="SELECT * FROM table WHERE date = '{{ params.i_start_date }}'")

elif dag_type == "마스터 DAG (Sub-DAG 호출)":
    d_col1, d_col2 = st.columns([2, 1])
    with d_col1:
        sub_dag_list = st.text_area("호출할 Sub-DAG ID 목록 (엔터로 구분)", height=150)
    with d_col2:
        poke_interval = st.number_input("Poke Interval (상태 확인 주기 - 초)", min_value=10, value=60)

elif dag_type == "프로시저 호출 (Stored Procedure)":
    d_col1, d_col2 = st.columns([1, 1.5])
    with d_col1:
        target_conn = st.text_input("GCP Conn CD", value="oned-etl")
    with d_col2:
        proc_name = st.text_input("프로시저명", placeholder="L1_ONED.SP_TRMRKT_GA_CMPGN...")

elif dag_type == "반복문 적재 (Loop ETL)":
    d_col1, d_col2, d_col3 = st.columns([1, 1.2, 1.5])
    with d_col1:
        target_db = st.selectbox("Target DB", ["BigQuery"])
        target_conn = st.text_input("Target Conn", value="oned-vertex")
    with d_col2:
        target_table = st.text_input("Target Table", placeholder="oned_ia.table_name_{{ item }}")
    with d_col3:
        loop_variables = st.text_input("반복 변수 목록 (쉼표로 구분)", placeholder="KR, US, EU")
    execute_query = st.text_area("추출 쿼리 (Loop 변수 적용)", height=150, placeholder="SELECT * FROM source_table WHERE country = '{{ item }}'")

elif dag_type == "커스텀 라이브러리 실행 (Python)":
    d_col1, d_col2 = st.columns(2)
    with d_col1:
        lib_module = st.text_input("모듈 경로 (Import Path)", placeholder="common.utils.data_loader")
    with d_col2:
        lib_func = st.text_input("실행 함수명", placeholder="process_complex_data")

elif dag_type == "태블로원본 추출 (Tableau)":
    st.info("💡 태블로 원본 추출은 하위 DAG로 사용되는 경우가 많습니다. 스케줄을 비워두시면 마스터 DAG에 의해서만 실행됩니다.")
    target_datasource = st.text_input("태블로 대상 원본명 (DataSource Name)", value="VSSALE_DAILY_OBS_ORD_STAT_AGGR_D")

st.markdown("---")

# ==========================================
# 3. Airflow 스케줄 및 옵션
# ==========================================
st.markdown("#### 3. Airflow 스케줄 및 옵션")
p_col1, p_col2, p_col3, p_col4 = st.columns([1.2, 1.3, 1, 1])

with p_col1:
    start_date = st.date_input("DAG 시작일 (Schedule Start)", value=datetime(2024, 5, 31))
    schedule_interval = st.text_input("Schedule (Cron)", value="", placeholder="0 8 * * * (비우면 None)")
    catchup = st.checkbox("Catchup (소급)", value=False)
        
with p_col2:
    auto_tags = st.checkbox("🏷️ 태그 자동 생성 (DAG ID 파싱)", value=True)
    tags = st.text_input("Tags (수동 입력)", value="oned_ia", disabled=auto_tags)
    render_template = st.checkbox("Render Template", value=True)
    wait_downstream = st.checkbox("Wait for Downstream", value=True)

with p_col3:
    enable_max_active_runs = st.checkbox("Max Active Runs 제한", value=True)
    max_active_runs = st.number_input("Max Runs 개수", min_value=1, value=1, disabled=not enable_max_active_runs)
    retries = st.number_input("Retries", min_value=0, value=1)
    retry_delay = st.number_input("Delay(분)", min_value=1, value=5)

with p_col4:
    st.markdown("**알림 (Callbacks)**")
    fail_alert = st.checkbox("🚨 실패 알림 (on_failure)", value=True)
    success_alert = st.checkbox("✅ 성공 알림 (on_success)", value=False)

st.markdown("---")

# ==========================================
# 4. 실행 시 파라미터
# ==========================================
st.markdown("#### 4. 실행 시 파라미터 (Runtime Params)")

if 'param_list' not in st.session_state:
    st.session_state.param_list = []
if 'param_counter' not in st.session_state:
    st.session_state.param_counter = 0

col_btn1, col_btn2, col_btn3 = st.columns([1, 1, 4])
with col_btn1:
    if st.button("➕ 일자 파라미터 추가", use_container_width=True):
        st.session_state.param_counter += 1
        st.session_state.param_list.append({"type": "date", "id": st.session_state.param_counter})
with col_btn2:
    if st.button("➕ 문자열 파라미터 추가", use_container_width=True):
        st.session_state.param_counter += 1
        st.session_state.param_list.append({"type": "string", "id": st.session_state.param_counter})

dag_params = {}
single_date_key = "i_date" 

has_start_id = next((p['id'] for p in st.session_state.param_list if p.get('is_start')), None)
has_end_id = next((p['id'] for p in st.session_state.param_list if p.get('is_end')), None)

for item in list(st.session_state.param_list):
    pid = item['id']
    with st.container():
        if item["type"] == "date":
            cols = st.columns([0.6, 0.6, 1.5, 1.2, 1.8, 0.6])
            with cols[0]:
                st.write("")
                if (has_start_id is None or has_start_id == pid) and not item.get('is_end', False):
                    is_start = st.checkbox("시작", value=item.get('is_start', False), key=f"chk_s_{pid}")
                    item['is_start'] = is_start
                else:
                    item['is_start'], is_start = False, False
            with cols[1]:
                st.write("")
                if (has_end_id is None or has_end_id == pid) and not item.get('is_start', False):
                    is_end = st.checkbox("종료", value=item.get('is_end', False), key=f"chk_e_{pid}")
                    item['is_end'] = is_end
                else:
                    item['is_end'], is_end = False, False
            
            with cols[2]:
                if is_start: param_key = "i_start_date"; st.text_input("파라미터명", value=param_key, disabled=True, key=f"dskey_{pid}")
                elif is_end: param_key = "i_end_date"; st.text_input("파라미터명", value=param_key, disabled=True, key=f"dekey_{pid}")
                else:
                    param_key = st.text_input("파라미터명", value=item.get('key_name', "i_date"), key=f"skey_{pid}")
                    item['key_name'] = param_key
                    if single_date_key == "i_date": single_date_key = param_key
            
            with cols[3]:
                p_type = st.selectbox("타입", ["고정값", "동적(Macro)"], key=f"ptype_{pid}")
            
            with cols[4]:
                if p_type == "고정값":
                    s_val = st.text_input("기본값 (YYYY-MM-DD)", value="2024-01-01", key=f"sval_{pid}")
                    dag_params[param_key] = {"type": "static", "default": s_val, "description": "일자 (고정)"}
                else:
                    c_dyn1, c_dyn2 = st.columns(2)
                    with c_dyn1: d_off = st.number_input("D- N일", value=1, key=f"doff_{pid}")
                    with c_dyn2: d_fmt = st.text_input("포맷", value="%Y%m%d", key=f"dfmt_{pid}")
                    dag_params[param_key] = {"type": "dynamic", "offset": d_off, "format": d_fmt, "description": "일자 (동적 매크로)"}

            with cols[5]:
                st.write("")
                if st.button("❌ 삭제", key=f"del_{pid}"):
                    st.session_state.param_list = [p for p in st.session_state.param_list if p['id'] != pid]
                    st.rerun()

        elif item["type"] == "string":
            cols = st.columns([1.2, 1.5, 3, 0.6])
            with cols[0]:
                st.write(""); st.markdown("**🔤 문자열**")
            with cols[1]:
                str_key = st.text_input("파라미터명", value=item.get('key_name', "i_param"), key=f"strkey_{pid}")
                item['key_name'] = str_key
            with cols[2]:
                str_val = st.text_input("기본값", value="ALL", key=f"strval_{pid}")
            
            dag_params[str_key] = {"type": "static", "default": str_val, "description": "문자열"}

            with cols[3]:
                st.write("")
                if st.button("❌ 삭제", key=f"del_{pid}"):
                    st.session_state.param_list = [p for p in st.session_state.param_list if p['id'] != pid]
                    st.rerun()

st.write("")
submitted = st.button("🚀 DAG 스크립트 생성하기", type="primary", use_container_width=True)

if submitted:
    if not dag_id:
        st.error("🚨 필수 항목(DAG ID)을 입력해주세요.")
    else:
        raw_schedule = schedule_interval.strip()
        schedule_val = "None" if not raw_schedule or raw_schedule.lower() == "none" else f"'{raw_schedule}'"
        
        catchup_val = "True" if catchup else "False"
        render_template_val = "True" if render_template else "False"
        wait_downstream_val = "True" if wait_downstream else "False"
        tags_list = [t.strip() for t in tags.split(",") if t.strip()]
        start_date_val = f"{start_date.year}, {start_date.month}, {start_date.day}"

        sum_schedule = raw_schedule if raw_schedule and raw_schedule.lower() != 'none' else '수동 실행 (None)'
        
        if dag_type == "표준 ETL (단일 쿼리 적재)":
            logic_summary = f"  - 파이프라인  : {source_db} -> {target_db}\n  - Target Table: {target_table}"
        elif dag_type == "마스터 DAG (Sub-DAG 호출)":
            logic_summary = f"  - 실행 방식   : Sub-DAG 트리거 (Poke: {poke_interval}초)"
        elif dag_type == "프로시저 호출 (Stored Procedure)":
            logic_summary = f"  - 프로시저명  : {proc_name}\n  - 파라미터    : 동적 매크로 연동 완료"
        elif dag_type == "반복문 적재 (Loop ETL)":
            logic_summary = f"  - 실행 방식   : Loop 기반 순차 적재\n  - 대상 변수   : {loop_variables}"
        elif dag_type == "태블로원본 추출 (Tableau)":
            logic_summary = f"  - 타겟 원본명 : {target_datasource}\n  - 실행 방식   : 태블로 TSC API 호출"
        else:
            logic_summary = f"  - 커스텀 로직 실행"

        etl_summary = (
            f"[기본 정보]\n"
            f"  - 프로젝트    : {project_name}\n"
            f"  - DAG ID      : {dag_id}\n"
            f"  - Owner       : {author} ({email})\n"
            f"  - Description : {description}\n"
            f"  - DAG 유형    : {dag_type}\n\n"
            f"[로직 및 연결 정보]\n{logic_summary}\n\n"
            f"[Airflow 파라미터]\n"
            f"  - Schedule    : {sum_schedule} (Start: {start_date.strftime('%Y-%m-%d')})\n"
            f"  - Callbacks   : Fail({fail_alert}), Success({success_alert})"
        )

        env = Environment(loader=FileSystemLoader('.'))
        
        try:
            if dag_type == "표준 ETL (단일 쿼리 적재)":
                template_file = 'templates/pattern_bq_to_bq.j2' if target_db == "BigQuery" else 'templates/pattern_bq_to_pg.j2'
            elif dag_type == "마스터 DAG (Sub-DAG 호출)": template_file = 'templates/pattern_master_dag.j2'
            elif dag_type == "프로시저 호출 (Stored Procedure)": template_file = 'templates/pattern_procedure.j2'
            elif dag_type == "반복문 적재 (Loop ETL)": template_file = 'templates/pattern_loop_etl.j2'
            elif dag_type == "커스텀 라이브러리 실행 (Python)": template_file = 'templates/pattern_custom_lib.j2'
            elif dag_type == "태블로원본 추출 (Tableau)": template_file = 'templates/pattern_tableau.j2'

            template = env.get_template(template_file)
            
            rendered_code = template.render(
                project_name=project_name, author=author, email=email, today_date=get_kst_now().strftime("%Y-%m-%d"),
                dag_id=dag_id, description=description,
                is_large_data=is_large_data, chunk_size=chunk_size,
                source_conn=source_conn, target_conn=target_conn,
                target_table=target_table, partition_column=partition_column, 
                execute_query=execute_query.replace('{{ item }}', '{item}'),
                start_date_val=start_date_val, schedule_val=schedule_val, catchup_val=catchup_val,
                retries=retries, retry_delay=retry_delay, tags_list=tags_list,
                enable_max_active_runs=enable_max_active_runs, max_active_runs=max_active_runs,
                render_template_val=render_template_val, wait_downstream_val=wait_downstream_val,
                dag_params=dag_params, single_date_key=single_date_key, etl_summary=etl_summary,
                source_db=source_db, target_db=target_db, sub_dag_list=sub_dag_list.split('\n') if sub_dag_list else [], 
                poke_interval=poke_interval, loop_variables=[v.strip() for v in loop_variables.split(',')] if loop_variables else [],
                proc_name=proc_name, lib_module=lib_module, lib_func=lib_func,
                target_datasource=target_datasource, 
                fail_alert=fail_alert, success_alert=success_alert, auto_tags=auto_tags
            )
            
            # ✅ [로깅] 스크립트 생성 시 DB에 로그 적재
            insert_log(
                user_id=st.session_state["user_id"],
                user_name=st.session_state["user_name"],
                event_name=f"GENERATE_{dag_type.split(' ')[0]}",
                script=rendered_code
            )
            
            st.success(f"✨ [{project_name}] {dag_type} 생성 완료! (생성 이력이 로깅되었습니다)")
            with st.expander("코드 미리보기", expanded=True): st.code(rendered_code, language='python')
            st.download_button("📥 파일 다운로드", rendered_code, file_name=f"{dag_id}.py")
            
        except Exception as e:
            st.error(f"⚠️ 템플릿 변환 중 오류가 발생했습니다: {e}")