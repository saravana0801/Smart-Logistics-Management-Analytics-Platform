import streamlit as st
from pathlib import Path
from dotenv import load_dotenv
import os
from sqlalchemy import create_engine, text
import pandas as pd

ROOT = Path(__file__).resolve().parents[1]
load_dotenv(ROOT / '.env')

DB_HOST = os.getenv('DB_HOST', 'localhost')
DB_PORT = os.getenv('DB_PORT', '3306')
DB_NAME = os.getenv('DB_NAME', 'logistics_db')
DB_USER = os.getenv('DB_USER', 'root')
DB_PASSWORD = os.getenv('DB_PASSWORD', '')

URL = f"mysql+pymysql://{DB_USER}:{DB_PASSWORD}@{DB_HOST}:{DB_PORT}/{DB_NAME}?charset=utf8mb4"
engine = create_engine(URL, pool_pre_ping=True)

st.set_page_config(page_title='Logistics Dashboard', layout='wide')
st.title('Smart Logistics Management & Analytics Platform')

# ============ HELPER FUNCTIONS ============


# @st.cache_data(ttl=120)
# def query_db(sql, params=None):
#     with engine.connect() as conn:
#         return pd.read_sql_query(text(sql), conn, params=params)

@st.cache_data(ttl=120)
def query_db(sql, params=None):
    try:
        with engine.connect() as conn:
            df = pd.read_sql_query(text(sql), conn, params=params)
        return df

    except Exception as e:
        st.error("Unable to fetch data. Please adjust filters or inputs.")
       # st.caption(f"Technical details: {e}")
        return pd.DataFrame()

@st.cache_data(ttl=600)
def get_couriers():
    return query_db('SELECT courier_id, name FROM courier_staff ORDER BY name')

def show_table(df):
    if df is not None and not df.empty:
        df = df.reset_index(drop=True)
        df.index += 1
    st.dataframe(df, use_container_width=True)


def show_insight_block(title, subtitle=None):
    st.markdown(f"### {title}")
    if subtitle:
        st.caption(subtitle)

def build_where(filters):
    clauses = []
    params = {}
    status = filters.get('status')
    origin = filters.get('origin')
    destination = filters.get('destination')
    courier = filters.get('courier')
    date_from = st.session_state.filters.get("date_from")
    date_to = st.session_state.filters.get("date_to")

    if date_from and date_to:
        if date_from > date_to:
            st.warning("⚠️ 'Date From' cannot be later than 'Date To'")
            st.stop()

    if status:
        statuses_quoted = ", ".join("'" + s.replace("'", "''") + "'" for s in status)
        clauses.append(f"s.status IN ({statuses_quoted})")
    if origin:
        clauses.append("s.origin LIKE :origin")
        params['origin'] = f"%{origin}%"
    if destination:
        clauses.append("s.destination LIKE :destination")
        params['destination'] = f"%{destination}%"
    if courier:
        clauses.append("s.courier_id = :courier")
        params['courier'] = courier
    if date_from:
        clauses.append("s.order_date >= :date_from")
        params['date_from'] = date_from
    if date_to:
        clauses.append("s.order_date <= :date_to")
        params['date_to'] = date_to

    where = (' WHERE ' + ' AND '.join(clauses)) if clauses else ''
    return where, params


def main():
    # ensure filter state exists
    if 'filters' not in st.session_state:
        st.session_state.filters = {
            'status': '',
            'origin': '',
            'destination': '',
            'courier': None,
            'date_from': None,
            'date_to': None,
        }
    if 'show_filters' not in st.session_state:
        st.session_state.show_filters = False

    # search on home page
    st.subheader('🔍 Search by shipment ID')
    sid = st.text_input('', key='sid')

    # filter icon/button
    if st.button('⚙️ Filters'):
        st.session_state.show_filters = True

    # filter section (inline, no box)
    if st.session_state.show_filters:
        st.write('**Adjust filters:**')
        col1, col2, col3, col4 = st.columns(4)
        with col1:
            st.session_state.filters['status'] = st.multiselect('Status', options=['Delivered', 'In Transit', 'Pending', 'Cancelled'])
        with col2:
            st.session_state.filters['origin'] = st.text_input('Origin', value=st.session_state.filters['origin'])
        with col3:
            st.session_state.filters['destination'] = st.text_input('Destination', value=st.session_state.filters['destination'])
        with col4:
            couriers = get_couriers()
            st.session_state.filters['courier'] = st.selectbox('Courier', options=[''] + couriers['courier_id'].tolist(), index=(0 if not st.session_state.filters['courier'] else couriers['courier_id'].tolist().index(st.session_state.filters['courier'])+1))
        
        col_f, col_t, col_btn = st.columns([1, 1, 1])
        with col_f:
            st.session_state.filters['date_from'] = st.date_input('Date From', value=st.session_state.filters['date_from'])
        with col_t:
            st.session_state.filters['date_to'] = st.date_input('Date To', value=st.session_state.filters['date_to'])
        with col_btn:
            st.write('')  # spacer
            if st.button('Apply'):
                st.session_state.show_filters = False
                if hasattr(st, 'rerun'):
                    st.rerun()
                else:
                    st.experimental_rerun()
    # compute where params from stored filters
    filters = st.session_state.filters
    where, params = build_where(filters)

    # sidebar navigation (button style)
    if 'section' not in st.session_state:
        st.session_state.section = 'Delivery Performance Insights'

    st.sidebar.markdown('### Analytics View')
    if st.sidebar.button('Delivery Performance Insights', use_container_width=True):
        st.session_state.section = 'Delivery Performance Insights'
    if st.sidebar.button('Courier Performance', use_container_width=True):
        st.session_state.section = 'Courier Performance'
    if st.sidebar.button('Cost Analytics', use_container_width=True):
        st.session_state.section = 'Cost Analytics'
    if st.sidebar.button('Cancellation Analysis', use_container_width=True):
        st.session_state.section = 'Cancellation Analysis'
    if st.sidebar.button('Warehouse Insights', use_container_width=True):
        st.session_state.section = 'Warehouse Insights'

    section = st.session_state.section

    # Top KPIs
    # handle delivered/cancelled clauses safely
    where_delivered = where + (" AND s.status='Delivered'" if where else " WHERE s.status='Delivered'")
    where_cancelled = where + (" AND s.status='Cancelled'" if where else " WHERE s.status='Cancelled'")

    q_total = f"SELECT COUNT(*) AS total FROM shipments s {where}"
    q_delivered = f"SELECT COUNT(*) AS delivered FROM shipments s {where_delivered}"
    q_cancelled = f"SELECT COUNT(*) AS cancelled FROM shipments s {where_cancelled}"

    df_total = query_db(q_total, params)
    total = int(df_total.iloc[0, 0]) if not df_total.empty else 0
    df_delivered = query_db(q_delivered, params)
    delivered = int(df_delivered.iloc[0, 0]) if not df_delivered.empty else 0
    df_cancelled = query_db(q_cancelled, params)
    cancelled = int(df_cancelled.iloc[0, 0]) if not df_cancelled.empty else 0

    df_avg = query_db(f"SELECT AVG(DATEDIFF(s.delivery_date, s.order_date)) AS avg_days FROM shipments s {where_delivered}", params)
    avg_time = float(df_avg.iloc[0]['avg_days']) if not df_avg.empty and df_avg.iloc[0]['avg_days'] is not None else 0

    df_cost = query_db(f"SELECT SUM(fuel_cost+labor_cost+misc_cost) AS total_cost FROM costs c JOIN shipments s USING(shipment_id) {where}", params)
    total_cost = float(df_cost.iloc[0]['total_cost']) if not df_cost.empty and df_cost.iloc[0]['total_cost'] is not None else 0

    col1, col2, col3 = st.columns(3)
    col1.metric('Total Shipments', total)
    col2.metric('Delivered %', f"{(delivered/total*100):.1f}%" if total else '0%')
    col3.metric('Cancelled %', f"{(cancelled/total*100):.1f}%" if total else '0%')

    col4, col5 = st.columns(2)
    col4.metric('Avg Delivery Days', f"{avg_time:.1f}" if avg_time else 'n/a')
    col5.metric('Total Cost', f"₹{total_cost:,.0f}" if total_cost else '₹0')

    # display KPIs summary
    st.markdown('---')
    if sid:
        try:
            df = query_db("SELECT * FROM shipments WHERE shipment_id = :sid", {'sid': sid})
            if df.empty:
                st.info('No shipment found for that ID')
            else:
                st.subheader('Shipment Details')
                # st.dataframe(df.T)
                show_table(df.T)
        except Exception as e:
            st.error(f'Query failed: {e}')

    # helper renderers
    def render_delivery():
        show_insight_block('Delivery Performance Insights', 'Charts first, detailed data on demand')
        q = f"SELECT s.origin,s.destination,AVG(DATEDIFF(s.delivery_date,s.order_date)) AS avg_days FROM shipments s WHERE s.delivery_date IS NOT NULL {('AND ' + where.lstrip(' WHERE ')) if where else ''} GROUP BY s.origin,s.destination"
        df = query_db(q, params)
        if not df.empty:
            df = df.sort_values('avg_days', ascending=False)
            df['route'] = df['origin'] + ' → ' + df['destination']
            left, right = st.columns([2, 1])
            with left:
                st.markdown('#### Average delivery time by route (Top 20)')
                st.bar_chart(df.head(20).set_index('route')['avg_days'])
            with right:
                st.markdown('#### Route summary')
                show_table(df[['origin', 'destination', 'avg_days']].head(20))
            with st.expander('View full route delivery table'):
                show_table(df[['origin', 'destination', 'avg_days']])
        
        st.markdown('#### Most delayed routes')
        q2 = f"SELECT s.origin,s.destination,AVG(DATEDIFF(s.delivery_date,s.order_date)) AS avg_days FROM shipments s WHERE s.delivery_date IS NOT NULL {('AND ' + where.lstrip(' WHERE ')) if where else ''} GROUP BY s.origin,s.destination ORDER BY avg_days DESC LIMIT 20"
        df2 = query_db(q2, params)
        if not df2.empty:
            df2['route'] = df2['origin'] + ' → ' + df2['destination']
            st.bar_chart(df2.set_index('route')['avg_days'])
            with st.expander('View delayed route table'):
                show_table(df2[['origin', 'destination', 'avg_days']])

        st.markdown('#### Delivery time vs distance')
        q3 = (
            "SELECT r.origin,r.destination,r.distance_km,AVG(DATEDIFF(s.delivery_date,s.order_date)) AS avg_days "
            "FROM shipments s JOIN routes r ON s.origin=r.origin AND s.destination=r.destination "
            f"WHERE s.delivery_date IS NOT NULL {('AND ' + where.lstrip(' WHERE ')) if where else ''} GROUP BY r.origin,r.destination,r.distance_km"
        )
        df3 = query_db(q3, params)
        if not df3.empty:
            st.scatter_chart(df3, x='distance_km', y='avg_days')
            with st.expander('View distance vs delivery table'):
                show_table(df3)

    def render_courier():
        show_insight_block('Courier Performance', 'Shipment load, delivery efficiency, and ratings')
        q = f"SELECT s.courier_id,c.name,COUNT(*) AS shipments FROM shipments s LEFT JOIN courier_staff c ON s.courier_id=c.courier_id {where} GROUP BY s.courier_id,c.name"
        df = query_db(q, params)
        if not df.empty:
            df = df.sort_values('shipments', ascending=False)
            left, right = st.columns([2, 1])
            with left:
                st.markdown('#### Shipments handled per courier (Top 20)')
                st.bar_chart(df.head(20).set_index('name')['shipments'])
            with right:
                st.markdown('#### Top couriers')
                show_table(df[['name', 'shipments']].head(10))
            with st.expander('View full courier shipment table'):
                show_table(df[['courier_id', 'name', 'shipments']])

        st.markdown('#### On-time delivery %')
        q2 = f"SELECT s.courier_id,c.name,ROUND(SUM(s.status='Delivered')/COUNT(*)*100,1) AS on_time_pct FROM shipments s LEFT JOIN courier_staff c ON s.courier_id=c.courier_id {where} GROUP BY s.courier_id,c.name"
        df2 = query_db(q2, params)
        if not df2.empty:
            df2 = df2.sort_values('on_time_pct', ascending=False)
            st.bar_chart(df2.head(20).set_index('name')['on_time_pct'])
            with st.expander('View on-time delivery table'):
                show_table(df2)

        st.markdown('#### Average rating comparison')
        q3 = "SELECT courier_id,name,rating FROM courier_staff ORDER BY rating DESC"
        ratings = query_db(q3)
        if not ratings.empty:
            left, right = st.columns([2, 1])
            with left:
                st.bar_chart(ratings.head(20).set_index('name')['rating'])
            with right:
                show_table(ratings[['name', 'rating']].head(10))
            with st.expander('View full courier ratings'):
                show_table(ratings)

    def render_cost():
        show_insight_block('Cost Analytics', 'Track route cost, contribution mix, and high-cost shipments')
        st.markdown('#### Total cost per shipment')
        q = "SELECT c.shipment_id,(fuel_cost+labor_cost+misc_cost) AS total_cost FROM costs c"
        df_cost_ship = query_db(q)
        if not df_cost_ship.empty:
            df_cost_ship = df_cost_ship.sort_values('total_cost', ascending=False)
            st.line_chart(df_cost_ship.head(100).set_index('shipment_id')['total_cost'])
            with st.expander('View shipment cost table'):
                show_table(df_cost_ship)

        st.markdown('#### Cost per route')
        q2 = f"SELECT s.origin,s.destination,AVG(fuel_cost+labor_cost+misc_cost) AS avg_cost FROM costs c JOIN shipments s ON c.shipment_id=s.shipment_id {where} GROUP BY s.origin,s.destination"
        route_cost = query_db(q2, params)
        if not route_cost.empty:
            route_cost = route_cost.sort_values('avg_cost', ascending=False)
            route_cost['route'] = route_cost['origin'] + ' → ' + route_cost['destination']
            left, right = st.columns([2, 1])
            with left:
                st.bar_chart(route_cost.head(20).set_index('route')['avg_cost'])
            with right:
                show_table(route_cost[['origin', 'destination', 'avg_cost']].head(10))
            with st.expander('View full route cost table'):
                show_table(route_cost[['origin', 'destination', 'avg_cost']])

        st.markdown('#### Fuel vs labor percentage contribution')
        cb = query_db("SELECT SUM(fuel_cost) AS fuel,SUM(labor_cost) AS labor,SUM(misc_cost) AS misc FROM costs")
        if not cb.empty:
            fuel, labor, misc = cb.iloc[0]
            total = fuel+labor+misc
            if total:
                c1, c2, c3 = st.columns(3)
                c1.metric('Fuel %', f"{fuel/total*100:.1f}%")
                c2.metric('Labor %', f"{labor/total*100:.1f}%")
                c3.metric('Misc %', f"{misc/total*100:.1f}%")

        st.markdown('#### High-cost shipments')
        high_cost = query_db("SELECT shipment_id,(fuel_cost+labor_cost+misc_cost) AS total_cost FROM costs ORDER BY total_cost DESC LIMIT 20")
        if not high_cost.empty:
            st.bar_chart(high_cost.set_index('shipment_id')['total_cost'])
            with st.expander('View high-cost shipment table'):
                show_table(high_cost)

    def render_cancellation():
        show_insight_block('Cancellation Analysis', 'Identify where and by whom cancellations happen')
        st.markdown('#### Cancellation rate by origin')
        q = f"SELECT s.origin,ROUND(SUM(s.status='Cancelled')/COUNT(*)*100,1) AS pct FROM shipments s {where} GROUP BY s.origin"
        by_origin = query_db(q, params)
        if not by_origin.empty:
            by_origin = by_origin.sort_values('pct', ascending=False)
            left, right = st.columns([2, 1])
            with left:
                st.bar_chart(by_origin.head(20).set_index('origin')['pct'])
            with right:
                show_table(by_origin.head(10))
            with st.expander('View full cancellation by origin table'):
                show_table(by_origin)

        st.markdown('#### Cancellation rate by courier')
        q2 = f"SELECT s.courier_id,ROUND(SUM(s.status='Cancelled')/COUNT(*)*100,1) AS pct FROM shipments s {where} GROUP BY s.courier_id"
        by_courier = query_db(q2, params)
        if not by_courier.empty:
            by_courier = by_courier.sort_values('pct', ascending=False)
            st.bar_chart(by_courier.head(20).set_index('courier_id')['pct'])
            with st.expander('View full cancellation by courier table'):
                show_table(by_courier)

        st.markdown('#### Time-to-cancellation analysis')
        q3 = "SELECT AVG(DATEDIFF(t.timestamp,s.order_date)) AS avg_days FROM shipments s JOIN shipment_tracking t ON s.shipment_id=t.shipment_id WHERE t.status='Cancelled'"
        df3 = query_db(q3)
        if not df3.empty:
            st.metric('Avg days to cancellation', f"{df3.iloc[0,0]:.1f}")

    def render_warehouses():
        show_insight_block('Warehouse Insights', 'Capacity and traffic trends by city')

        st.markdown('#### Warehouse capacity comparison')
        wh = query_db("SELECT city,capacity FROM warehouses")
        if not wh.empty:
            left, right = st.columns([2, 1])
            with left:
                st.bar_chart(wh.set_index('city')['capacity'])
            with right:
                show_table(wh.sort_values('capacity', ascending=False).head(10))

        st.markdown('#### High-traffic warehouse cities')
        traffic = query_db("SELECT s.origin AS city,COUNT(*) AS shipments FROM shipments s JOIN warehouses w ON s.origin=w.city GROUP BY s.origin ORDER BY shipments DESC LIMIT 10")
        if not traffic.empty:
            st.bar_chart(traffic.set_index('city')['shipments'])
            with st.expander('View warehouse traffic table'):
                show_table(traffic)

    # dispatch based on sidebar choice
    if section == 'Delivery Performance Insights':
        render_delivery()
    elif section == 'Courier Performance':
        render_courier()
    elif section == 'Cost Analytics':
        render_cost()
    elif section == 'Cancellation Analysis':
        render_cancellation()
    elif section == 'Warehouse Insights':
        render_warehouses()


if __name__ == '__main__':
    main()
    
