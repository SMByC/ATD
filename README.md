# Automatización de las Alertas Tempranas de Deforestación #

El programa ATD hace parte del proceso de alertas tempranas de deforestación, uno de los componentes del Sistema de Monitoreo de Bosques y Carbono realizado por el IDEAM para Colombia.

## Los dos esquemas del monitoreo de deforestación ##

El monitoreo de deforestación, desarrolla dos productos a distintas escalas de tiempo y resolución espacial, uno  genera alertas tempranas indicando donde se están presentando las áreas deforestadas y el otro genera el mapa de cambio de cobertura boscosas. Para el primer caso, hace uso de imágenes satelitales MODIS de escala gruesa pero alta resolución temporal y se realiza cada 6 meses, el segundo usa las imágenes satelitales Landsat de escala fina pero baja resolución temporal y se realiza cada año.

A continuación se describen los dos esquemas de monitoreo de la deforestación de Colombia a nivel nacional: 

* Escala gruesa usa imágenes de sensores remotos de baja resolución espacial, cuyos tamaños de pixel oscilan entre 250m-500m, y que proveen una alta resolución temporal generando información muy actualizada, y permitiendo identificar rápidamente las áreas de cambio (“Hotspots” en deforestación) en las coberturas de Bosque/ No Bosque. Esta escala aplicada en un monitoreo anual permite hacer un seguimiento periódico, constituyéndose un sistema de alerta temprana en las dinámicas de cambio de este tipo de coberturas. Así mismo, los productos espectrales generados apoyarán la estimación del almacenamiento de carbono a nivel nacional

* Escala fina usa imágenes de sensores remotos de media resolución espacial (imágenes tipo Landsat TM y ETM+), cuyos tamaños de pixel oscilan entre 15m-60m, las cuales permiten tener un cubrimiento completo del país con un buen nivel de detalle espacial para la identificación de coberturas de la Tierra. 

Ambos procesos requieren de varias etapas, aunque similares, difieren en varios aspectos metodológicos, ambos procesos, de manera muy general, requieren de 3 fases: 

1. La descarga de la información: Imágenes satelitales (MODIS, Landsat).

2. El procesamiento de la información: Diversos cálculos usando las bandas espectrales de la imágenes, calculo de estadísticos, generación de mosaicos, etc. 

3. Generación del producto: Creación de los mapas definitivos usando los producto de los mapas generados del proceso anterior, comparándolos o no con resultados de procesos anteriores, aplicando filtros, etc. 

## Automatización de Alertas Tempranas de Deforestación (ATD) ##

El programa ATD es el proceso de automatización de alertas tempranas de deforestación (escala gruesa y alta resolución temporal). Este proceso de automatización se encarga de realizar toda la descarga y procesamiento de la información (ítems 1 y 2) cuyas tareas son automatizables ya que su metodología no varia de un producto a otro. La generación final del producto (ítem 3) es un proceso complejo de automatización ya que requiere de análisis y conocimientos de un especialista. El proceso ATD surge con la idea de operatizar y automatizar todas las tareas y procesos posibles dentro de la metodología de alertas tempranas que no requieran de conocimiento experto, con el fin de realizar las alertas tempranas de manera mas eficiente y veloz, facilitando su elaboración y ejecución, evitando errores y demoras en tareas repetitivas u operacionales.

El programa ATD cuenta con los siguientes procesos (en la actualidad):

0. Download
1. Tiseq
2. MRT
3. NoData
4. Stats
5. NoData
6. Mosaic
7. Layerstack

Una vez realizado todo el proceso de ATD, los resultados del Layerstack (último proceso) son comparados y analizados con el producto del anterior periodo, aplicando filtros y análsis de un especialista, que realiza los productos finales.

## Team ##

- Xavier Corredor Llano <xcorredorl(a)ideam.gov.co>
- Edersson Cabrera <cabrera.edersson(a)gmail.com>
- Gustavo Galindo <gusgalin(a)gmail.com>
- Cristhian Forero <cristhian0224(a)gmail.com>
- Juan Pablo Ramirez <juanramirez85(a)gmail.com>

***
    Copyright © 2014-2015 IDEAM
    Instituto de Hidrología, Meteorología y Estudios Ambientales
    Sistema de Monitoreo de Bosques y Carbono - SMBYC
    Calle 25 D No. 96 B - 70
    Bogotá DC, Colombia
