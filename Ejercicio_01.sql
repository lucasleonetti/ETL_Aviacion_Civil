--- Cantidad de vuelos entre las fechas 01/12/2021 y 31/01/2022.
SELECT COUNT(*) AS cantidad_vuelos
FROM aeropuerto_tabla
WHERE fecha BETWEEN '2021-12-01' AND '2022-01-31';
--- Cantidad de pasajeros de Aerolíneas Argentinas entre el 01/01/2021 y 30/06/2022
SELECT SUM(pasajeros) AS total_pasajeros
FROM aeropuerto_tabla
WHERE aerolinea_nombre = 'AEROLINEAS ARGENTINAS SA'
    AND fecha BETWEEN '2021-01-01' AND '2022-06-30';



--- Fecha, hora, código aeropuerto salida, ciudad de salida, código de aeropuerto
--- de arribo, ciudad de arribo, y cantidad de pasajeros de cada vuelo, entre el 01/01/2022
--- y el 30/06/2022 ordenados por fecha de manera descendiente.
SELECT vuelos.fecha,
    vuelos.hora_utc,
    vuelos.aeropuerto AS codigo_aeropuerto_salida,
    salida.denominacion AS ciudad_salida,
    CASE
        WHEN vuelos.tipo_de_movimiento = 'Aterrizaje' THEN vuelos.aeropuerto
        ELSE vuelos.origen_destino
    END AS codigo_aeropuerto_arribo,
    CASE
        WHEN vuelos.tipo_de_movimiento = 'Aterrizaje' THEN llegada.denominacion
        ELSE salida.denominacion
    END AS ciudad_arribo,
    vuelos.pasajeros
FROM aeropuerto_tabla AS vuelos
    LEFT JOIN aeropuerto_detalles_tabla AS salida ON vuelos.aeropuerto = salida.aeropuerto
    LEFT JOIN aeropuerto_detalles_tabla AS llegada ON vuelos.origen_destino = llegada.aeropuerto
WHERE vuelos.fecha BETWEEN '2022-01-01' AND '2022-06-30'
ORDER BY vuelos.fecha DESC;



--- las 10 aerolíneas que más pasajeros llevaron entre el 01/01/2021 y el
--- 30/06/2022 exceptuando aquellas aerolíneas que no tengan nombre
SELECT aerolinea_nombre,
    SUM(pasajeros) AS total_pasajeros
FROM aeropuerto_tabla
WHERE fecha BETWEEN '2021-01-01' AND '2022-06-30'
    AND aerolinea_nombre IS NOT NULL
    AND aerolinea_nombre != ''
GROUP BY aerolinea_nombre
ORDER BY total_pasajeros DESC
LIMIT 10;



---  las 10 aeronaves más utilizadas entre el 01/01/2021 y el 30/06/22 que
--- despegaron desde la Ciudad autónoma de Buenos Aires o de Buenos Aires,
--- exceptuando aquellas aeronaves que no cuentan con nombre.
SELECT aeronave,
    COUNT(*) AS total_vuelos
FROM aeropuerto_tabla
WHERE fecha BETWEEN '2021-01-01' AND '2022-06-30'
    AND LOWER(origen_destino) IN ('aep', 'eze') -- Filtra por códigos de Aeroparque (AEP) y Ezeiza (EZE)
    AND aeronave IS NOT NULL
    AND aeronave != ''
GROUP BY aeronave
ORDER BY total_vuelos DESC
LIMIT 10;