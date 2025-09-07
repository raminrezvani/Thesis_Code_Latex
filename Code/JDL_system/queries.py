from typing import Dict, Iterable, Iterator, Tuple

from rdflib import Graph, Literal, Namespace, RDF, URIRef
from rdflib.namespace import XSD

Triple = Tuple[str, str, str]


TRAFFIC_NS = "http://example.org/traffic#"
CITY_NS = "http://example.org/cityOnto#"


def _build_graph_from_triples(triples: Iterable[Triple]) -> Graph:
	g = Graph()
	traffic = Namespace(TRAFFIC_NS)
	city = Namespace(CITY_NS)
	g.bind("traffic", traffic)
	g.bind("city", city)

	for s, p, o in triples:
		# Subject
		subj = URIRef(s) if s.startswith("http") else URIRef(s)
		# Predicate
		if p == "rdf:type":
			pred = RDF.type
		elif p.startswith("http"):
			pred = URIRef(p)
		else:
			pred = URIRef(p)
		# Object
		if isinstance(o, str) and o.startswith("http"):
			obj = URIRef(o)
		else:
			# try to parse numeric
			try:
				if "." in o:
					obj = Literal(float(o), datatype=XSD.float)
				else:
					obj = Literal(int(o), datatype=XSD.integer)
			except Exception:
				obj = Literal(o)
		g.add((subj, pred, obj))
	return g


def append_triples_to_graph(g: Graph, triples: Iterable[Triple]) -> None:
	traffic = Namespace(TRAFFIC_NS)
	city = Namespace(CITY_NS)
	for s, p, o in triples:
		subj = URIRef(s) if s.startswith("http") else URIRef(s)
		if p == "rdf:type":
			pred = RDF.type
		elif p.startswith("http"):
			pred = URIRef(p)
		else:
			pred = URIRef(p)
		if isinstance(o, str) and o.startswith("http"):
			obj = URIRef(o)
		else:
			try:
				if "." in o:
					obj = Literal(float(o), datatype=XSD.float)
				else:
					obj = Literal(int(o), datatype=XSD.integer)
			except Exception:
				obj = Literal(o)
		g.add((subj, pred, obj))


def q1_vehicle_count_observations(triples: Iterable[Triple]) -> Iterator[Triple]:
	"""Q1 (C-SPARQL-like via rdflib): vehicle count per location using SPARQL.

	We SELECT counts and materialize Observation triples.
	"""
	g = _build_graph_from_triples(triples)
	query = f"""
	PREFIX traffic: <{TRAFFIC_NS}>
	PREFIX xsd: <http://www.w3.org/2001/XMLSchema#>
	SELECT ?location (COUNT(?vehicle) AS ?vehicleCount)
	WHERE {{
		?vehicle a traffic:Vehicle ;
			traffic:atLocation ?location .
	}}
	GROUP BY ?location
	"""
	for row in g.query(query):
		location_uri = str(row[0])
		count_val = int(row[1].toPython())
		obs = f"{TRAFFIC_NS}observation/vehicle_count_{location_uri.rsplit('/', 1)[-1]}"
		yield (obs, "rdf:type", f"{TRAFFIC_NS}Observation")
		yield (obs, f"{TRAFFIC_NS}hasObservationType", "VehicleCount")
		yield (obs, f"{TRAFFIC_NS}atLocation", location_uri)
		yield (obs, f"{TRAFFIC_NS}hasVehicleCount", str(count_val))


def q2_situation_refinement(triples: Iterable[Triple], threshold: int = 100) -> Iterator[Triple]:
	"""Q2 (C-SPARQL-like via rdflib): situation refinement using SPARQL aggregation.

	Aggregates VehicleCount observations per location and emits TrafficJam situations
	when SUM(count) > threshold.
	"""
	g = _build_graph_from_triples(triples)
	query = f"""
	PREFIX traffic: <{TRAFFIC_NS}>
	PREFIX xsd: <http://www.w3.org/2001/XMLSchema#>
	SELECT ?location (SUM(xsd:integer(?vc)) AS ?total)
	WHERE {{
		?obs a traffic:Observation ;
			traffic:hasObservationType "VehicleCount" ;
			traffic:atLocation ?location ;
			traffic:hasVehicleCount ?vc .
	}}
	GROUP BY ?location
	HAVING (SUM(xsd:integer(?vc)) > {threshold})
	"""
	for row in g.query(query):
		location_uri = str(row[0])
		total_val = int(row[1].toPython())
		situation = f"{TRAFFIC_NS}situation/traffic_jam_{location_uri.rsplit('/', 1)[-1]}"
		yield (situation, f"{TRAFFIC_NS}hasSituationType", "TrafficJam")
		yield (situation, f"{TRAFFIC_NS}atLocation", location_uri)
		yield (situation, f"{TRAFFIC_NS}hasVehicleCount", str(total_val))


def q2_situation_refinement_over_graph(g: Graph, threshold: int = 100) -> Iterator[Triple]:
	query = f"""
	PREFIX traffic: <{TRAFFIC_NS}>
	PREFIX xsd: <http://www.w3.org/2001/XMLSchema#>
	SELECT ?location (SUM(xsd:integer(?vc)) AS ?total)
	WHERE {{
		?obs a traffic:Observation ;
			traffic:hasObservationType "VehicleCount" ;
			traffic:atLocation ?location ;
			traffic:hasVehicleCount ?vc .
	}}
	GROUP BY ?location
	HAVING (SUM(xsd:integer(?vc)) > {threshold})
	"""
	for row in g.query(query):
		location_uri = str(row[0])
		total_val = int(row[1].toPython())
		situation = f"{TRAFFIC_NS}situation/traffic_jam_{location_uri.rsplit('/', 1)[-1]}"
		yield (situation, f"{TRAFFIC_NS}hasSituationType", "TrafficJam")
		yield (situation, f"{TRAFFIC_NS}atLocation", location_uri)
		yield (situation, f"{TRAFFIC_NS}hasVehicleCount", str(total_val))


