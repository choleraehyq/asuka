all:
	cd cmd/cm && go build
	cd cmd/dn && go build
	cd cmd/example && go build
clean:
	cd cmd/cm && rm cm
	cd cmd/dn && rm dn
	cd cmd/example && rm example 
