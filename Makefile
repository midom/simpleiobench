CFLAGS+=`pkg-config glib-2.0 gthread-2.0 --cflags` -Wall -std=c99
LDFLAGS+=`pkg-config glib-2.0 gthread-2.0 --libs`

all: simpleiobench

install: simpleiobench
	install -D simpleiobench ${DESTDIR}/usr/local/bin/simpleiobench

clean:
	rm -f simpleiobench *~
