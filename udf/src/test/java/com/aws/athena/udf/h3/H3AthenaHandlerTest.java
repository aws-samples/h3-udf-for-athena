package com.aws.athena.udf.h3;

import com.uber.h3core.util.GeoCoord;
import com.uber.h3core.LengthUnit;


import junit.framework.Test;
import junit.framework.TestCase;
import junit.framework.TestSuite;
import java.io.IOException;
import com.uber.h3core.H3Core;
import java.util.LinkedList;
import java.util.List;
import java.util.Random;
/**
 * Unit test for simple App.
 */
public class H3AthenaHandlerTest 
    extends TestCase
{
    final private H3AthenaHandler handler;

    final private H3Core h3Core;

    /**
     * Create the test case
     *
     * @param testName name of the test case
     */
    public H3AthenaHandlerTest( String testName ) throws IOException
    {

        super( testName );
        handler = new H3AthenaHandler();
        h3Core = H3Core.newInstance();
    }

    /**
     * @return the suite of tests being tested
     */
    public static Test suite()
    {
        return new TestSuite( H3AthenaHandlerTest.class );
    }

    public void testgeo_to_h3() 
    {
        
        assertNull(handler.geo_to_h3(null, 10.5, 1));
        assertNull(handler.geo_to_h3(-10.4, null, 2));
        assertNull(handler.geo_to_h3(10.4, 13.2, null));


        Random r = new Random();

        for (int i=0; i < 1000; ++i) {

            double latitude = (Math.random() * 180.0) - 90.0;
            double longitude = (Math.random() * 360.0) - 180.0;

            double latitudeOther =  Math.max(-90.0, latitude - Math.random());
            double longitudeOther = Math.max(-180.0, longitude- Math.random());

            int res = r.nextInt(15);

            assertTrue( handler.geo_to_h3(latitude, longitude,res) == h3Core.geoToH3(latitude, longitude, res));

            if (handler.geo_to_h3(latitude, longitude,res).longValue() == handler.geo_to_h3(latitudeOther, longitudeOther,res)) {
                assertTrue(handler.geo_to_h3(latitude, longitude,res).longValue()  == h3Core.geoToH3(latitudeOther, longitudeOther,res));
            }
            else {
                assertFalse(handler.geo_to_h3(latitude, longitude,res)  == h3Core.geoToH3(latitudeOther, longitudeOther,res));

            }

            
        }
    }

    public void testgeo_to_h3_address() 
    {
        
        assertNull(handler.geo_to_h3_address(null, 10.5, 1));
        assertNull(handler.geo_to_h3_address(-10.4, null, 2));
        assertNull(handler.geo_to_h3_address(10.4, 13.2, null));


        Random r = new Random();

        for (int i=0; i < 1000; ++i) {

            double latitude = (Math.random() * 180.0) - 90.0;
            double longitude = (Math.random() * 360.0) - 180.0;

            double latitudeOther =  Math.max(-90.0, latitude - Math.random());
            double longitudeOther = Math.max(-180.0, longitude- Math.random());

            int res = r.nextInt(15);

            assertEquals( handler.geo_to_h3_address(latitude, longitude,res) , h3Core.geoToH3Address(latitude, longitude, res));

            if (handler.geo_to_h3_address(latitude, longitude,res).equals(handler.geo_to_h3_address(latitudeOther, longitudeOther,res))) {
                assertEquals(handler.geo_to_h3_address(latitude, longitude,res), h3Core.geoToH3Address(latitudeOther, longitudeOther,res));
            }
            else {
                assertFalse(handler.geo_to_h3_address(latitude, longitude,res).equals(h3Core.geoToH3Address(latitudeOther, longitudeOther,res)));

            }
            
        }
    }

    public void testh3_to_geo() {
        assertNull(handler.h3_to_geo((Long)null));

        final Random r = new Random();

        for (int i=0; i < 1000; ++i) {
            double latitude = (Math.random() * 180.0) - 90.0;
            double longitude = (Math.random() * 360.0) - 180.0;

            int res = r.nextInt(15);


            // The centroid geo returns by a centroid is the centroid itself.
            final Long h3 = handler.geo_to_h3(latitude, longitude, res);
            final List<Double> geo = handler.h3_to_geo(h3);
            final Long centroid = handler.geo_to_h3(geo.get(0), geo.get(1), res);
 
            assertEquals(handler.h3_to_geo(centroid), geo);
        }
    }

    public void testh3_to_geo_wkt() {
        assertNull(handler.h3_to_geo_wkt((Long)null));

        final double latitude = 50.0;
        final double longitude = -43;
        final Long h3 = handler.geo_to_h3(latitude, longitude, 4);


        assertEquals(handler.h3_to_geo_wkt(h3), "POINT (50.166306 -42.941921)");

    }

    public void testh3_to_geo_boundary() {

        assertNull(handler.h3_to_geo_boundary((Long)null, ","));
        
        final double latitude = 50.0;
        final double longitude = -43;
        final Long h3 = handler.geo_to_h3(latitude, longitude, 4);

        for (int i = 0; i < 6; ++i) {
            
            String[] splittedResult = handler.h3_to_geo_boundary(h3, ",").get(i).split(",");
            
            assertEquals(h3Core.h3ToGeoBoundary(h3).get(i).lat , Double.parseDouble(splittedResult[0]), 1e-4);
            assertEquals(h3Core.h3ToGeoBoundary(h3).get(i).lng , Double.parseDouble(splittedResult[1]), 1e-4);
        }       
    }

    public void testpoint_dist() {
        final double distance1 = handler.point_dist("POINT (43.552847 7.017369)","POINT (47.218371 -1.55362)", "rads" );
        final double distance2 = handler.point_dist("POINT(43.552847 7.017369)","POINT (47.218371 -1.55362)", "rads" );


        assertEquals(h3Core.pointDist(
                                new GeoCoord(43.552847, 7.017369), 
                                new GeoCoord(47.218371,-1.55362), 
                                LengthUnit.valueOf("rads")),
                      distance1,
                      1e-3);
        assertEquals(distance2, distance1, 1e-3);

    }

    public void testpolyfill() {
        final String polygonWKT = "POLYGON((43.604652 1.444209, 47.218371 -1.553621, 50.62925 3.05726, 48.864716 2.349014, 43.6961 7.27178, 43.604652 1.444209))";
        final List<GeoCoord> geoCoordPoints = List.of(new GeoCoord(43.604652,1.444209),
                                                      new GeoCoord(47.218371, -1.553621),
                                                      new GeoCoord(50.62925, 3.05726),
                                                      new GeoCoord(48.864716, 2.349014),
                                                      new GeoCoord(43.6961, 7.27178),
                                                      new GeoCoord(43.604652, 1.444209));
        final String polygonWKTAlt = "POLYGON  ((43.604652 1.444209, 47.218371 -1.553621, 50.62925 3.05726, 48.864716 2.349014, 43.6961 7.27178, 43.604652 1.444209))";

        final List<List<GeoCoord>>  empty = new LinkedList<>();

        for (int i = 0; i <= 5 ;++i) {
            assertEquals(h3Core.polyfill(geoCoordPoints, empty, i), handler.polyfill(polygonWKT, i));
            assertEquals(handler.polyfill(polygonWKT, i), handler.polyfill(polygonWKTAlt, i));

        }
    }

    public void testpolyfill_address() {
        final String polygonWKT = "POLYGON((43.604652 1.444209, 47.218371 -1.553621, 50.62925 3.05726, 48.864716 2.349014, 43.6961 7.27178, 43.604652 1.444209))";
        final List<GeoCoord> geoCoordPoints = List.of(new GeoCoord(43.604652,1.444209),
                                                      new GeoCoord(47.218371, -1.553621),
                                                      new GeoCoord(50.62925, 3.05726),
                                                      new GeoCoord(48.864716, 2.349014),
                                                      new GeoCoord(43.6961, 7.27178),
                                                      new GeoCoord(43.604652, 1.444209));
        final String polygonWKTAlt = "POLYGON ((43.604652 1.444209, 47.218371 -1.553621, 50.62925 3.05726, 48.864716 2.349014, 43.6961 7.27178, 43.604652 1.444209))";

        final List<List<GeoCoord>>  empty = new LinkedList<>();

        for (int i = 0; i <= 5 ;++i) {
            assertEquals(h3Core.polyfillAddress(geoCoordPoints, empty, i), handler.polyfill_address(polygonWKT, i));
            assertEquals(handler.polyfill_address(polygonWKT, i), handler.polyfill_address(polygonWKTAlt, i));

        }
    }

    public void testh3_set_to_multipolygon() {
        final List<Long> h3_indexes = List.of( 613498908116516863L,
                                               613499565410091007L,
                                               613498908185722879L,
                                               613499565420576767L,
                                               613498908183625727L,
                                               613499565418479615L,
                                               613498908145876991L,
                                               613498908223471615L,
                                               613498907535605759L,
                                               613498908120711167L,
                                               613498908124905471L,
                                               613498908217180159L,
                                               613498908112322559L,
                                               613498908156362751L,
                                               613498908215083007L,
                                               613498908221374463L,
                                               613498908118614015L,
                                               613498908212985855L,
                                               613498908154265599L,
                                               613499565414285311L,
                                               613499565412188159L,
                                               613498908158459903L,
                                               613499565416382463L,
                                               613498908225568767L,
                                               613498908219277311L,
                                               613499565422673919L );
        List<List<List<GeoCoord>>> polygon1 = h3Core.h3SetToMultiPolygon(h3_indexes, true);
        String polygon2 = handler.h3_set_to_multipolygon(h3_indexes, true);
        String polygon2Split[] = polygon2.substring("MULTIPOLYGON (((".length(), polygon2.length() - 3).split("\\)\\), \\(\\(");

        assertEquals(polygon2Split.length, polygon1.size());

        for (int i = 0; i < polygon2Split.length; ++i) {
            String[] splitted = polygon2Split[i].split(",");
            assertEquals(splitted.length, polygon1.get(i).get(0).size());

            for (int j = 0; j < splitted.length ;  ++j) {
                String[] latlng = splitted[j].trim().split(" ");
                assertEquals(Double.parseDouble(latlng[0]), polygon1.get(i).get(0).get(j).lat, 1e-3);
                assertEquals(Double.parseDouble(latlng[1]), polygon1.get(i).get(0).get(j).lng, 1e-3);

            }

        }
        assertEquals(polygon2Split.length, polygon1.size());
        assertEquals(2, polygon2Split.length);
        
    }


}
