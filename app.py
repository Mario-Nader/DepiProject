import streamlit as st
import pandas as pd
import numpy as np
from datetime import datetime, time
from sklearn.preprocessing import StandardScaler, OneHotEncoder
from sklearn.compose import ColumnTransformer
from sklearn.pipeline import Pipeline
from sklearn.ensemble import RandomForestClassifier
import pyodbc
import plotly.express as px

# Set page configuration
st.set_page_config(
    page_title="تنبؤ حركة المرور",
    page_icon="🚦",
    layout="wide",
    initial_sidebar_state="expanded"
)

# Custom CSS for RTL and styling
st.markdown("""
    <style>
        .main {
            direction: rtl;
            text-align: right;
            font-family: 'Arial', sans-serif;
        }
        .stButton>button {
            width: 100%;
            border-radius: 20px;
            background-color: #4CAF50;
            color: white;
            font-weight: bold;
            padding: 10px 24px;
        }
        .stTextInput>div>div>input, .stSelectbox>div>div>select {
            text-align: right;
            direction: rtl;
        }
        .header {
            text-align: center;
            color: #2c3e50;
            padding: 20px 0;
        }
        .prediction-box {
            background-color: #f8f9fa;
            border-radius: 10px;
            padding: 20px;
            margin: 20px 0;
            text-align: center;
            box-shadow: 0 4px 6px rgba(0, 0, 0, 0.1);
        }
        .prediction-text {
            font-size: 24px;
            font-weight: bold;
            margin: 10px 0;
        }
        .traffic-heavy { color: #e74c3c; }
        .traffic-high { color: #e67e22; }
        .traffic-normal { color: #3498db; }
        .traffic-low { color: #2ecc71; }
    </style>
""", unsafe_allow_html=True)

@st.cache_data
def load_data():
    server = 'depiser'
    database = 'traffic'
    username = 'dbtuser'
    password = 'AM.most123'
    driver = '{ODBC Driver 17 for SQL Server}'
    table_name = 'dbo.stg_traffic'

    try:
        conn_str = f'DRIVER={driver};SERVER={server};DATABASE={database};UID={username};PWD={password}'
        with pyodbc.connect(conn_str, timeout=30) as cnxn:
            query = f"SELECT * FROM {table_name}"
            df = pd.read_sql(query, cnxn)
        return df
    except pyodbc.Error as ex:
        st.error(f"حدث خطأ في الاتصال بقاعدة البيانات: {ex}")
        return None
    except Exception as e:
        st.error(f"حدث خطأ غير متوقع: {e}")
        return None

def preprocess_data(df):
    df['Hour'] = df['traffic_time'].apply(lambda t: t.hour if hasattr(t, 'hour') else int(str(t).split(':')[0]))
    df['DayOfWeek'] = df['day_of_week'].astype(str)
    df.ffill(inplace=True)
    return df

def train_model(df):
    X = df[['CarCount', 'BikeCount', 'BusCount', 'TruckCount', 'Hour', 'DayOfWeek']]
    y = df['traffic_situation']
    
    numeric_features = ['CarCount', 'BikeCount', 'BusCount', 'TruckCount', 'Hour']
    categorical_features = ['DayOfWeek']
    
    numeric_transformer = Pipeline([('scaler', StandardScaler())])
    categorical_transformer = Pipeline([('onehot', OneHotEncoder(handle_unknown='ignore'))])
    
    preprocessor = ColumnTransformer(
        transformers=[
            ('num', numeric_transformer, numeric_features),
            ('cat', categorical_transformer, categorical_features)
        ])
    
    model = Pipeline([
        ('preprocessor', preprocessor),
        ('classifier', RandomForestClassifier(n_estimators=100, random_state=42))
    ])
    
    model.fit(X, y)
    return model

def get_traffic_level(prediction):
    return {1: "ثقيل", 2: "مرتفع", 3: "عادي", 4: "منخفض"}.get(prediction, "غير معروف")

def get_traffic_class(prediction):
    return {1: "traffic-heavy", 2: "traffic-high", 3: "traffic-normal", 4: "traffic-low"}.get(prediction, "")

def main():
    st.markdown("<h1 class='header'>🚦 نظام تنبؤ حركة المرور</h1>", unsafe_allow_html=True)
    
    df = load_data()
    if df is None:
        return
    
    df = preprocess_data(df)
    model = train_model(df)
    
    with st.sidebar:
        st.header("إدخال البيانات")
        selected_time = st.time_input("الوقت", value=datetime.now().time())
        days = ["الإثنين", "الثلاثاء", "الأربعاء", "الخميس", "الجمعة", "السبت", "الأحد"]
        selected_day = st.selectbox("اليوم", days, index=datetime.now().weekday())
        day_mapping = {day: idx for idx, day in enumerate(days)}
        
        st.subheader("عدد المركبات")
        car_count = st.slider("عدد السيارات", 0, 500, 50)
        bike_count = st.slider("عدد الدراجات النارية", 0, 200, 20)
        bus_count = st.slider("عدد الحافلات", 0, 100, 5)
        truck_count = st.slider("عدد الشاحنات", 0, 100, 3)
        
        predict_button = st.button("تنبأ بحركة المرور")
    
    col1, col2 = st.columns([2,1])
    
    with col1:
        st.subheader("تحليل حركة المرور")
        summary_data = pd.DataFrame({
            'نوع المركبة': ['سيارات','دراجات نارية','حافلات','شاحنات'],
            'العدد': [car_count,bike_count,bus_count,truck_count]
        })
        fig = px.bar(summary_data, x='نوع المركبة', y='العدد', 
                     title='توزيع المركبات',
                     color='نوع المركبة',
                     color_discrete_sequence=px.colors.qualitative.Pastel)
        st.plotly_chart(fig, use_container_width=True)
        
        st.subheader("تحليل الوقت")
        time_data = df.groupby('Hour')['total_vehicles'].mean().reset_index()
        fig = px.line(time_data, x='Hour', y='total_vehicles',
                     title='متوسط حركة المرور حسب الساعة',
                     labels={'Hour':'الساعة','total_vehicles':'متوسط عدد المركبات'})
        st.plotly_chart(fig, use_container_width=True)
    
    with col2:
        if predict_button:
            input_data = pd.DataFrame({
                'CarCount':[car_count],
                'BikeCount':[bike_count],
                'BusCount':[bus_count],
                'TruckCount':[truck_count],
                'Hour':[selected_time.hour],
                'DayOfWeek':[str(day_mapping[selected_day])]  # تحويل إلى string
            })
            prediction = model.predict(input_data)[0]
            traffic_level = get_traffic_level(prediction)
            traffic_class = get_traffic_class(prediction)
            
            st.markdown(f"""
                <div class='prediction-box'>
                    <h3>حالة حركة المرور المتوقعة:</h3>
                    <div class='prediction-text {traffic_class}'>{traffic_level}</div>
                    <p>الوقت: {selected_time.strftime('%I:%M %p')}</p>
                    <p>اليوم: {selected_day}</p>
                </div>
            """, unsafe_allow_html=True)
            
            st.subheader("نصائح المرور")
            if prediction == 1:
                st.warning("حركة مرور كثيفة. يُنصح باستخدام طرق بديلة أو تأجيل الرحلة إذا أمكن.")
            elif prediction == 2:
                st.info("حركة مرور عالية. قد تواجه ازدحامًا خفيفًا.")
            elif prediction == 3:
                st.success("حركة مرور عادية. استمتع برحلتك!")
            else:
                st.success("حركة مرور خفيفة. الطريق مفتوح.")

if __name__ == "__main__":
    main()