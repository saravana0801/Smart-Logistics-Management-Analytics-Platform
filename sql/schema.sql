DROP TABLE IF EXISTS shipment_tracking;
DROP TABLE IF EXISTS costs;
DROP TABLE IF EXISTS shipments;
DROP TABLE IF EXISTS courier_staff;
DROP TABLE IF EXISTS routes;
DROP TABLE IF EXISTS warehouses;


CREATE TABLE courier_staff (
  courier_id VARCHAR(50) PRIMARY KEY,
  name VARCHAR(150),
  rating DECIMAL(3,1),
  vehicle_type VARCHAR(50)
);

CREATE TABLE routes (
  route_id VARCHAR(50) PRIMARY KEY,
  origin VARCHAR(100) NOT NULL,
  destination VARCHAR(100) NOT NULL,
  distance_km DECIMAL(10,2),
  avg_time_hours DECIMAL(5,2)
);

CREATE TABLE warehouses (
  warehouse_id VARCHAR(50) PRIMARY KEY,
  city VARCHAR(100) NOT NULL,
  state VARCHAR(50),
  capacity INT
) ;

CREATE TABLE shipments (
    shipment_id VARCHAR(50) PRIMARY KEY,
    order_date DATE,
    origin VARCHAR(100),
    destination VARCHAR(100),
    weight DECIMAL(10, 2),
    courier_id VARCHAR(50),
    status VARCHAR(50),
    delivery_date DATE NULL,
    CONSTRAINT fk_shipments_courier
        FOREIGN KEY (courier_id)
        REFERENCES courier_staff(courier_id)
);

CREATE TABLE costs (
  shipment_id VARCHAR(50) PRIMARY KEY,
  fuel_cost DECIMAL(15,2),
  labor_cost DECIMAL(15,2),
  misc_cost DECIMAL(15,2),
  CONSTRAINT fk_costs_shipment 
      FOREIGN KEY (shipment_id) 
      REFERENCES shipments(shipment_id)
);

CREATE TABLE shipment_tracking (
  tracking_id INT PRIMARY KEY,
  shipment_id VARCHAR(50),
  status VARCHAR(50),
  timestamp DATETIME,
  CONSTRAINT fk_tracking_shipment FOREIGN KEY (shipment_id) REFERENCES shipments(shipment_id) ON DELETE CASCADE ON UPDATE CASCADE
);

CREATE INDEX idx_shipments_origin_dest ON shipments (origin, destination);
CREATE INDEX idx_shipments_status ON shipments (status);
CREATE INDEX idx_tracking_shipment ON shipment_tracking (shipment_id)

