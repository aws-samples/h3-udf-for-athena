package com.aws.athena.udf.h3;

import junit.framework.Test;
import junit.framework.TestCase;
import junit.framework.TestSuite;
import java.io.IOException;
import com.uber.h3core.H3Core;
import java.util.Random;
import java.util.List;
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

        Random r = new Random();

        for (int i=0; i < 1000; ++i) {
            double latitude = (Math.random() * 180.0) - 90.0;
            double longitude = (Math.random() * 360.0) - 180.0;

            int res = r.nextInt(15);


            // The centroid geo returns by a centroid is the centroid itself.
            Long h3 = handler.geo_to_h3(latitude, longitude, res);
            List<Double> geo = handler.h3_to_geo(h3);
            Long centroid = handler.geo_to_h3(geo.get(0), geo.get(1), res);
 
            assertEquals(handler.h3_to_geo(centroid), geo);
        }
    }

    public void testh3_to_geo_wkt() {
        assertNull(handler.h3_to_geo_wkt((Long)null));

        double latitude = 50.0;
        double longitude = -43;
        Long h3 = handler.geo_to_h3(latitude, longitude, 4);


        assertEquals(handler.h3_to_geo_wkt(h3), "POINT (50.166306 -42.941921)");

    }

    public void testh3_to_geo_boundary() {

        assertNull(handler.h3_to_geo_boundary((Long)null, ","));
        
        double latitude = 50.0;
        double longitude = -43;
        Long h3 = handler.geo_to_h3(latitude, longitude, 4);

        for (int i = 0; i < 6; ++i) {
            
            String[] splittedResult = handler.h3_to_geo_boundary(h3, ",").get(i).split(",");
            
            System.out.println(handler.h3_to_geo_boundary(h3, ",").get(i));
            System.out.println(h3Core.h3ToGeoBoundary(h3).get(i).lat);
            System.out.println(h3Core.h3ToGeoBoundary(h3).get(i).lng);


            assertEquals(h3Core.h3ToGeoBoundary(h3).get(i).lat , Double.parseDouble(splittedResult[0]), 1e-4);
            assertEquals(h3Core.h3ToGeoBoundary(h3).get(i).lng , Double.parseDouble(splittedResult[1]), 1e-4);
        }       
    }

}
