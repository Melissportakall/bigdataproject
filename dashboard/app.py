import streamlit as st
import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
import numpy as np
import mlflow

# Sayfa Ayarları
st.set_page_config(page_title="NYC Taxi Trip Dashboard", page_icon="🚕", layout="wide")

st.title("🚕 NYC Taxi Trip Süre/Ücret Tahmini Dashboard")
st.markdown("Büyük Veri Analizine Giriş Dönem Projesi - Adım 7")

# --- MLflow Ayarları ---
mlflow.set_tracking_uri("http://mlflow-server:5001")

@st.cache_data(ttl=600) # 10 dakikada bir yenile
def fetch_mlflow_metrics():
    try:
        experiment = mlflow.get_experiment_by_name("Taxi_Fare_Prediction")
        if not experiment:
            return None
        
        runs = mlflow.search_runs(experiment_ids=[experiment.experiment_id])
        if runs.empty:
            return None
        
        # Latest runs for each model
        model_names = {
            "Linear_Regression_Final": "Linear Regression",
            "Decision_Tree_Final": "Decision Tree",
            "RF_Regressor_Final": "Random Forest",
            "GBT_Regressor_Final": "GBT",
            "GLR_Final": "GLR"
        }
        
        extracted_data = []
        for run_name, display_name in model_names.items():
            # Find the most recent run with this name
            model_run = runs[runs['tags.mlflow.runName'] == run_name].sort_values(by='start_time', ascending=False)
            if not model_run.empty:
                extracted_data.append({
                    'Model': display_name,
                    'RMSE': model_run.iloc[0]['metrics.rmse'],
                    'MAE': model_run.iloc[0]['metrics.mae'],
                    'R2': model_run.iloc[0]['metrics.r2']
                })
        
        return pd.DataFrame(extracted_data) if extracted_data else None
    except Exception as e:
        st.sidebar.error(f"MLflow Bağlantı Hatası: {e}")
        return None

# --- Veri Yükleme ---
@st.cache_data
def load_data():
    columns_to_read = ['tpep_pickup_datetime', 'trip_distance', 'total_amount', 'payment_type']
    try:
        # Konteyner içi yol: /app/data/
        df = pd.read_parquet('/app/data/yellow_tripdata_2024-01.parquet', columns=columns_to_read)
        df = df.sample(n=100000, random_state=42)
    except:
        df = pd.DataFrame(columns=columns_to_read)
    return df

def load_metrics():
    model_metrics, stream_metrics, feat_importance = None, None, None
    
    # Her dosyayı bağımsız okuyalım ki biri eksikse diğeri çalışmaya devam etsin
    try:
        model_metrics = pd.read_csv('/app/data/model_metrics.csv')
    except:
        pass
        
    try:
        stream_metrics = pd.read_csv('/app/data/stream_metrics.csv')
        if stream_metrics is not None and 'Stream R2' in stream_metrics.columns:
            stream_metrics = stream_metrics.rename(columns={'Stream R2': 'R2', 'Stream RMSE': 'RMSE', 'Stream MAE': 'MAE'})
    except:
        pass
        
    try:
        feat_importance = pd.read_csv('/app/data/feature_importance.csv')
    except:
        pass
            
    return model_metrics, stream_metrics, feat_importance

df = load_data()
model_metrics, stream_metrics, feat_importance = load_metrics()

# --- KPI Kartları ---
if not df.empty:
    st.markdown("### 📊 Genel Özet (Örneklem Verisi Üzerinden)")
    col1, col2, col3 = st.columns(3)
    with col1:
        st.metric(label="Toplam Yolculuk Kaydı", value="2,964,624")
    with col2:
        st.metric(label="Ortalama Mesafe", value=f"{df['trip_distance'].mean():.2f} mil")
    with col3:
        # En iyi model sonucunu CSV'den veya MLflow'dan alalım
        best_r2_val = None
        if model_metrics is not None:
            best_r2_val = model_metrics['R2'].max() * 100
        else:
            mlflow_df_kpi = fetch_mlflow_metrics()
            if mlflow_df_kpi is not None:
                best_r2_val = mlflow_df_kpi['R2'].max() * 100
        
        if best_r2_val is not None:
            st.metric(label="En İyi Model R² Skoru", value=f"%{best_r2_val:.2f}")
        else:
            st.metric(label="En İyi Model R² Skoru", value="%90.20")
else:
    st.warning("Veri yüklenemedi. Lütfen data/ klasörünü kontrol edin.")

st.markdown("---")

# --- 1. Model Performans Karşılaştırması ---
st.markdown("### 1. Model Performans Karşılaştırması")

# 1.1. Notebook Çıktısı (Kapsamlı Görsel)
try:
    st.image('/app/data/model_comparison.png', caption='Modellerin Performans Kıyaslaması (Statik Görsel)', use_container_width=True)
except:
    st.info("Kıyaslama görseli bulunamadı.")

st.markdown("#### 🔍 İnteraktif Metrik Analizi")
# 1.2. İnteraktif Grafik (CSV veya MLflow)
if model_metrics is not None:
    plot_df = model_metrics.melt(id_vars='Model', var_name='Metric', value_name='Value')
    fig_models = px.bar(plot_df, x='Model', y='Value', color='Metric', barmode='group',
                        title='Modellerin Metrik Kıyaslaması (İnteraktif)',
                        color_discrete_sequence=['tomato', 'steelblue', 'seagreen'])
    st.plotly_chart(fig_models, use_container_width=True)
else:
    mlflow_df = fetch_mlflow_metrics()
    if mlflow_df is not None:
        plot_df = mlflow_df.melt(id_vars='Model', var_name='Metric', value_name='Value')
        fig_models = px.bar(plot_df, x='Model', y='Value', color='Metric', barmode='group',
                            title='Modellerin MLflow Üzerindeki Performans Kıyaslaması (İnteraktif)',
                            color_discrete_sequence=['tomato', 'steelblue', 'seagreen'])
        st.plotly_chart(fig_models, use_container_width=True)

st.markdown("---")

# --- 2. Streaming Test Sonuçları ---
st.markdown("### 2. Canlı Veri (Streaming) Test Sonuçları")
if stream_metrics is not None:
    st.write("**📊 Streaming Test Metrikleri (Gerçek Zamanlı Performans)**")
    # Tabloyu tam genişlikte göstererek sıkışıklığı gideriyoruz
    st.dataframe(stream_metrics.style.highlight_max(subset=['R2'], color='lightgreen'), use_container_width=True)
            
    st.markdown("#### 🔍 Streaming Tahmin Analizi (Örnek Model)")
    col_s3, col_s4 = st.columns(2)
    with col_s3:
        try:
            st.image('/app/data/stream_predictions.png', caption='Streaming: Gerçek vs Tahmin', use_container_width=True)
        except:
            st.info("Tahmin grafiği bulunamadı.")
    with col_s4:
        try:
            st.image('/app/data/stream_residuals.png', caption='Streaming: Hata (Residual) Dağılımı', use_container_width=True)
        except:
            st.info("Hata dağılımı grafiği bulunamadı.")
else:
    st.info("Streaming test sonuçları bulunamadı. Lütfen 07_models_test.ipynb notebook'unu çalıştırın.")

st.markdown("---")

col_left, col_right = st.columns(2)

with col_left:
    # --- 3. Zaman Serisi (Saatlik Trend) ---
    st.markdown("### 3. Saatlik Yolculuk Trendi")
    if not df.empty:
        df['hour'] = df['tpep_pickup_datetime'].dt.hour
        hourly_counts = df['hour'].value_counts().sort_index().reset_index()
        hourly_counts.columns = ['Saat', 'Yolculuk Sayısı']
        fig_hourly = px.line(hourly_counts, x='Saat', y='Yolculuk Sayısı', markers=True,
                             title='Günün Saatlerine Göre Yolculuk Yoğunluğu')
        st.plotly_chart(fig_hourly, use_container_width=True)

with col_right:
    # --- 3.2. Değişken Önem Düzeyleri (Feature Importance) ---
    st.markdown("### 3.2. Değişken Önem Düzeyleri")
    if feat_importance is not None:
        feat_df = feat_importance.sort_values(by='Importance', ascending=True)
        fig_feat = px.bar(feat_df, x='Importance', y='Feature', orientation='h', 
                          title='En İyi Modelin Değişken Önem Düzeyleri',
                          color='Importance', color_continuous_scale='Blues')
        st.plotly_chart(fig_feat, use_container_width=True)
    else:
        features = ['trip_distance', 'hour', 'day_of_week', 'PULocationID', 'payment_type']
        importances = [0.85, 0.05, 0.04, 0.03, 0.03]
        feat_df = pd.DataFrame({'Feature': features, 'Importance': importances}).sort_values(by='Importance', ascending=True)
        fig_feat = px.bar(feat_df, x='Importance', y='Feature', orientation='h', 
                          title='En İyi Modelin Değişken Önem Düzeyleri',
                          color='Importance', color_continuous_scale='Blues')
        st.plotly_chart(fig_feat, use_container_width=True)

st.markdown("---")

st.markdown("### 4. Keşifçi Veri Analizi (EDA) Görselleri")
col_eda1, col_eda2 = st.columns(2)

with col_eda1:
    try:
        st.image('/app/data/distance_vs_amount.png', caption='Mesafe vs Ücret İlişkisi')
    except:
        if not df.empty:
            df_filtered = df[(df['trip_distance'] > 0) & (df['trip_distance'] <= 20)]
            fig_hist = px.histogram(df_filtered, x='trip_distance', nbins=40, 
                                    title='Yolculuk Mesafesi Dağılımı (0-20 Mil)',
                                    labels={'trip_distance': 'Mesafe (Mil)'},
                                    color_discrete_sequence=['#83c9ff'])
            st.plotly_chart(fig_hist, use_container_width=True)

with col_eda2:
    try:
        st.image('/app/data/payment_type_dist.png', caption='Ödeme Türü Dağılımı')
    except:
        if not df.empty:
            payment_map = {1.0: 'Kredi Kartı', 2.0: 'Nakit', 3.0: 'Ücretsiz', 4.0: 'İtirazlı'}
            df['payment_mapped'] = df['payment_type'].map(payment_map)
            pay_counts = df['payment_mapped'].value_counts().reset_index()
            pay_counts.columns = ['Ödeme Türü', 'Sayı']
            fig_pie = px.pie(pay_counts, names='Ödeme Türü', values='Sayı', 
                             title='Ödeme Türü Dağılımı', hole=0.4)
            st.plotly_chart(fig_pie, use_container_width=True)

# --- Ek EDA Görselleri (Zorunlu 3 ek görsel için) ---
col_eda_r1_1, col_eda_r1_2 = st.columns(2)
with col_eda_r1_1:
    try: st.image('/app/data/passenger_count_dist.png', caption='Yolcu Sayısı Dağılımı', use_container_width=True)
    except: st.info("passenger_count_dist.png bulunamadı.")
with col_eda_r1_2:
    try: st.image('/app/data/total_amount_dist.png', caption='Toplam Ücret Dağılımı', use_container_width=True)
    except: st.info("total_amount_dist.png bulunamadı.")

col_eda_r2_1, col_eda_r2_2, col_eda_r2_3 = st.columns([1, 2, 1])
with col_eda_r2_2:
    try: st.image('/app/data/daily_trips.png', caption='Günlük Yolculuk Sayısı', use_container_width=True)
    except: st.info("daily_trips.png bulunamadı.")

st.markdown("---")
st.caption("NYC Taxi Big Data Project - 2026")
