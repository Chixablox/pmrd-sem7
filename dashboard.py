import streamlit as st
import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
from sqlalchemy import create_engine, text
from datetime import datetime, timedelta

from config import DATABASE_URL


@st.cache_data(ttl=300)
def load_data(limit):
    engine = create_engine(DATABASE_URL)
    q = text(
        "select check_id, check_type, table_name, execution_date, status, error_message "
        "from s_psql_dds.t_dq_check_results "
        "order by execution_date desc limit :limit"
    )
    df = pd.read_sql(q, engine, params={"limit": limit})
    if not df.empty:
        df['execution_date'] = pd.to_datetime(df['execution_date'])
    return df


def create_status_pie_chart(df):
    status_counts = df['status'].value_counts().reset_index()
    status_counts.columns = ['status', 'count']
    
    colors = {
        'passed': '#28a745',
        'failed': '#dc3545',
        'error': '#ffc107',
        'cancelled': '#6c757d'
    }
    status_counts['color'] = status_counts['status'].map(colors)
    
    fig = px.pie(
        status_counts, 
        values='count', 
        names='status',
        title='Распределение статусов проверок',
        color='status',
        color_discrete_map=colors,
        hole=0.4
    )
    fig.update_traces(textposition='inside', textinfo='percent+label+value')
    return fig


def create_check_type_bar_chart(df):
    check_status = df.groupby(['check_type', 'status']).size().reset_index(name='count')
    
    fig = px.bar(
        check_status,
        x='check_type',
        y='count',
        color='status',
        title='Результаты проверок по типам',
        labels={'check_type': 'Тип проверки', 'count': 'Количество', 'status': 'Статус'},
        color_discrete_map={
            'passed': '#28a745',
            'failed': '#dc3545',
            'error': '#ffc107',
            'cancelled': '#6c757d'
        },
        barmode='group'
    )
    fig.update_layout(xaxis_tickangle=-45)
    return fig


def create_timeline_chart(df):
    df_sorted = df.sort_values('execution_date')
    timeline = df_sorted.groupby([pd.Grouper(key='execution_date', freq='1min'), 'status']).size().reset_index(name='count')
    
    fig = px.line(
        timeline,
        x='execution_date',
        y='count',
        color='status',
        title='Динамика проверок во времени',
        labels={'execution_date': 'Время выполнения', 'count': 'Количество', 'status': 'Статус'},
        color_discrete_map={
            'passed': '#28a745',
            'failed': '#dc3545',
            'error': '#ffc107',
            'cancelled': '#6c757d'
        }
    )
    return fig


def create_success_rate_gauge(df):
    total = len(df)
    passed = len(df[df['status'] == 'passed'])
    success_rate = (passed / total * 100) if total > 0 else 0
    
    fig = go.Figure(go.Indicator(
        mode="gauge+number+delta",
        value=success_rate,
        domain={'x': [0, 1], 'y': [0, 1]},
        title={'text': "Процент успешных проверок"},
        delta={'reference': 95, 'suffix': '%'},
        gauge={
            'axis': {'range': [None, 100]},
            'bar': {'color': "darkblue"},
            'steps': [
                {'range': [0, 50], 'color': "#ffcccc"},
                {'range': [50, 80], 'color': "#fff4cc"},
                {'range': [80, 95], 'color': "#cce5ff"},
                {'range': [95, 100], 'color': "#ccffcc"}
            ],
            'threshold': {
                'line': {'color': "red", 'width': 4},
                'thickness': 0.75,
                'value': 95
            }
        }
    ))
    return fig


def main():
    st.set_page_config(page_title="Data Quality Dashboard", layout="wide", page_icon="")
    
    st.title("Data Quality Dashboard")
    st.markdown("Дашборд для мониторинга качества данных в `s_psql_dds.t_dq_check_results`")
    
    st.sidebar.header("Настройки")
    limit = st.sidebar.number_input(
        "Количество записей", 
        min_value=50, 
        max_value=5000, 
        value=500,
        step=50
    )
    
    with st.spinner('Загрузка данных...'):
        df = load_data(limit)
    
    if df.empty:
        st.warning("Нет данных о проверках качества")
        st.info("Убедитесь, что функция `fn_dq_checks_load()` была запущена и результаты записаны в таблицу.")
        return
    
    st.sidebar.header("Фильтры")
    
    all_statuses = ['Все'] + list(df['status'].unique())
    selected_status = st.sidebar.selectbox("Статус", all_statuses)
    
    all_check_types = ['Все'] + list(df['check_type'].unique())
    selected_check_type = st.sidebar.selectbox("Тип проверки", all_check_types)
    
    filtered_df = df.copy()
    if selected_status != 'Все':
        filtered_df = filtered_df[filtered_df['status'] == selected_status]
    if selected_check_type != 'Все':
        filtered_df = filtered_df[filtered_df['check_type'] == selected_check_type]
    
    col1, col2, col3, col4 = st.columns(4)
    
    with col1:
        st.metric("Всего проверок", len(filtered_df))
    
    with col2:
        passed_count = len(filtered_df[filtered_df['status'] == 'passed'])
        st.metric("Успешно", passed_count, delta=None)
    
    with col3:
        failed_count = len(filtered_df[filtered_df['status'] == 'failed'])
        st.metric("Провалено", failed_count, delta=None)
    
    with col4:
        success_rate = (passed_count / len(filtered_df) * 100) if len(filtered_df) > 0 else 0
        st.metric("Процент успеха", f"{success_rate:.1f}%")
    
    st.divider()
    
    tab1, tab2, tab3, tab4 = st.tabs(["Обзор", "По типам проверок", "Временная динамика", "Данные"])
    
    with tab1:
        col1, col2 = st.columns(2)
        
        with col1:
            st.plotly_chart(create_status_pie_chart(filtered_df), use_container_width=True)
        
        with col2:
            st.plotly_chart(create_success_rate_gauge(filtered_df), use_container_width=True)
        
        st.subheader("Последние проверки")
        latest_checks = filtered_df.head(10)[['execution_date', 'check_type', 'table_name', 'status', 'error_message']]
        st.dataframe(latest_checks, use_container_width=True, hide_index=True)
    
    with tab2:
        st.plotly_chart(create_check_type_bar_chart(filtered_df), use_container_width=True)
        
        st.subheader("Статистика по типам проверок")
        check_stats = filtered_df.groupby(['check_type', 'status']).size().reset_index(name='count')
        check_pivot = check_stats.pivot(index='check_type', columns='status', values='count').fillna(0)
        st.dataframe(check_pivot, use_container_width=True)
    
    with tab3:
        st.plotly_chart(create_timeline_chart(filtered_df), use_container_width=True)
        
        st.subheader("Проверки по датам")
        filtered_df['date'] = filtered_df['execution_date'].dt.date
        date_stats = filtered_df.groupby(['date', 'status']).size().reset_index(name='count')
        date_pivot = date_stats.pivot(index='date', columns='status', values='count').fillna(0)
        st.dataframe(date_pivot, use_container_width=True)
    
    with tab4:
        st.subheader("Проваленные проверки")
        failed_checks = filtered_df[filtered_df['status'] == 'failed']
        if not failed_checks.empty:
            st.dataframe(
                failed_checks[['execution_date', 'check_type', 'table_name', 'error_message']],
                use_container_width=True,
                hide_index=True
            )
        else:
            st.success("Нет проваленных проверок!")
        
        st.divider()
        
        st.subheader("Все проверки")
        st.dataframe(filtered_df, use_container_width=True, hide_index=True)
        
        csv = filtered_df.to_csv(index=False).encode('utf-8')
        st.download_button(
            label="Скачать данные (CSV)",
            data=csv,
            file_name=f"dq_checks_{datetime.now().strftime('%Y%m%d_%H%M%S')}.csv",
            mime="text/csv"
        )


if __name__ == '__main__':
    main()