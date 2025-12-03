Aufgabe: Entscheidung zwischen öffis oder PKW aufgrund von Wetterdaten

Architekturidee:

(Bild zur architektur hier)

Oder Flink

Verteilt: spark → läuft immer verteilt, Cluster von N rechnern, 1 Rechner ist auch N. 

Lake House → Storage skalieren, Public Cloud, Studenten guthaben, Limit aufpassen. 

TODO:  
\- Alle APIs auf JSON statt CSV  
\- TomTom api 5 cities  
	\= Wie machen wir das? 1x pro city, oder mehrere und dann Durchschnitt?  
	\= Hamburg, Berlin, Frankfurt, Muenchen, Koeln,  
	Duesseldorf, Hannover, Stuttgart, Nuernberg, Essen,  
	Leipzig, Dortmund, Bremen, Dresden, Mannheim  
\- Weather api auf 5 städte, geodaten (lat, long)  
\- Alles erstmal Bronze

\- Automate scripts fuer data  
\- Start architecture :)

\- Ueberlegen wie wir DB und TomTom daten vergleichen?  
z.B. soll-ist KM/H \=\> arbeitsweg \=\> wv minuten verspaetung? ⇐⇒ vergleich mit DB versp.  
Oder: % congestion mit % verspaetung der zuege vergleichen