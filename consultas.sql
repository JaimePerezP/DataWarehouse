USE `datawarehouse-youtube`;
/*Número de vídeos en tendencias por país*/
SELECT pais, COUNT(id_video) num_videos
FROM HECHOS
GROUP BY pais
ORDER BY num_videos;

/*Hora más frecuente en la que se suben vídeos*/
SELECT hora, MAX(num_videos)
FROM (
	SELECT HOUR(t.hora_publi) hora, COUNT(h.id_video) num_videos
    FROM Dimension_tiempo t, Hechos h
    WHERE h.id_tiempo = t.id_tiempo
    GROUP BY hora) AS v;

/*Canal con más vídeos en tendencias*/
SELECT titulo_canal, MAX(num_videos)
FROM (
	SELECT titulo_canal, COUNT(id_video) num_videos
    FROM Hechos
    GROUP BY titulo_canal) AS v;

/*Categoría más popular*/
SELECT nombre_categoria, MAX(num_videos)
FROM (
	SELECT nombre_categoria, COUNT(id_video) num_videos
    FROM Hechos
    GROUP BY nombre_categoria) AS v;

/*Número de vídeos que han desactivado los comentarios y estadísticas*/
SELECT COUNT(id_video)
FROM Dimension_video
WHERE comments_disabled = 1 AND retings_disabled = 1;

/*Tiempo medio en días de llegada a tendencias por categorías*/
SELECT h.nombre_categoria, AVG(t.dia_trend-t.dia_publi) media
FROM Hechos h, Dimension_tiempo t
WHERE h.id_tiempo = t.id_tiempo
GROUP BY nombre_categoria
ORDER BY media;