package com.aws.athena.udf.h3;

import com.uber.h3core.util.GeoCoord;
import com.uber.h3core.H3Core;
import com.uber.h3core.LengthUnit;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.io.IOException;
import java.util.Arrays;
import java.util.LinkedList;
import java.util.List;
import java.util.Random;
/**
 * Unit test for simple App.
 */
public class H3AthenaHandlerTest 
{
    final private H3AthenaHandler handler;

    final private H3Core h3Core;

    /**
     * Create the test case
     *
     * @param testName name of the test case
     */
    public H3AthenaHandlerTest() throws IOException
    {
        handler = new H3AthenaHandler();
        h3Core = H3Core.newInstance();
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

    public void testh3_get_faces() {

        final double latitude = 43.0;
        final double longitude = -42;

        assertNull(handler.h3_get_faces((Long)null));
        assertNull(handler.h3_get_faces((String)null));

        for (int i = 0; i < 16; ++i) {
            final Long h3 = handler.geo_to_h3(latitude, longitude, i);
            final Long h3Address = handler.geo_to_h3(latitude, longitude, i);

            assertEquals(h3Core.h3GetFaces(h3), handler.h3_get_faces(h3));
            assertEquals(h3Core.h3GetFaces(h3Address), handler.h3_get_faces(h3Address));

        }

    }


    public void testk_ring() throws Exception {
        final double latitude = 43.0;
        final double longitude = -42;

        assertNull(handler.k_ring((Long)null, 1));
        assertNull(handler.k_ring((Long)null, null));

        assertNull(handler.k_ring((String)null, 2));
        assertNull(handler.k_ring((String)null, null));

        final Long h3 = handler.geo_to_h3(latitude, longitude, 3);
        final String h3Address = handler.geo_to_h3_address(latitude, longitude, 3);

        assertNull(handler.k_ring(h3, null));
        assertNull(handler.k_ring(h3Address, null));


        final int k_max = 5;

        for (int i = 1; i < k_max; ++i) {
            List<Long> indexes = handler.k_ring(h3, i);
            for (long index : indexes) {
                assertTrue(handler.h3_distance(index, h3) <= k_max);
            }
        }

        for (int i = 1; i < k_max; ++i) {
            List<String> addresses = handler.k_ring(h3Address, i);
            for (String address : addresses) {
                assertTrue(handler.h3_distance(address, h3Address) <= k_max);
            }
        }
    }

    public void testhex_ring() throws Exception {
        final double latitude = 43.0;
        final double longitude = -42;

        assertNull(handler.hex_ring((Long)null, 1));
        assertNull(handler.hex_ring((Long)null, null));

        assertNull(handler.hex_ring((String)null, 2));
        assertNull(handler.hex_ring((String)null, null));

        final Long h3 = handler.geo_to_h3(latitude, longitude, 3);
        final String h3Address = handler.geo_to_h3_address(latitude, longitude, 3);

        assertNull(handler.hex_ring(h3, null));
        assertNull(handler.hex_ring(h3Address, null));


        final int k_max = 5;

        for (int i = 1; i < k_max; ++i) {
            List<Long> indexes = handler.hex_ring(h3, i);
            for (long index : indexes) {
                assertTrue(handler.h3_distance(index, h3) <= k_max);
            }
        }

        for (int i = 1; i < k_max; ++i) {
            List<String> addresses = handler.hex_ring(h3Address, i);
            for (String address : addresses) {
                assertTrue(handler.h3_distance(address, h3Address) <= k_max);
            }
        }
    }



    public void test_pentagons() {
        final double latitude = 43.0;
        final double longitude = -42;

        for (int i = 0; i < 16; ++i) {
            final Long h3 = handler.geo_to_h3(latitude, longitude, i);
            final Long h3Address = handler.geo_to_h3(latitude, longitude, i);
            assertFalse(handler.h3_is_pentagon((Long)null));
            assertFalse(handler.h3_is_pentagon((String)null));
      
            List<Long> indexes = handler.get_pentagon_indexes(i);
            List<String> addresses = handler.get_pentagon_adresses(i);

            assertEquals(h3Core.getPentagonIndexes(i), indexes);
            assertEquals(h3Core.getPentagonIndexesAddresses(i), addresses);


            assertFalse(handler.h3_is_pentagon(h3));
            assertFalse(handler.h3_is_pentagon(h3Address));
            assertEquals(h3Core.h3IsPentagon(h3), handler.h3_is_pentagon(h3));
            assertEquals(h3Core.h3IsPentagon(h3Address), handler.h3_is_pentagon(h3Address));
            for (Long index : indexes) {
                assertTrue(handler.h3_is_pentagon(index));
                assertEquals(h3Core.h3IsPentagon(index), handler.h3_is_pentagon(index));
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

            final String h3Address = handler.geo_to_h3_address(latitude, longitude, res);
            final List<Double> geoFromAddress = handler.h3_to_geo(h3Address);
            final Long centroidFromAddress = handler.geo_to_h3(geo.get(0), geo.get(1), res);

            assertEquals(handler.h3_to_geo(centroidFromAddress), geoFromAddress);
            
        }
    }

    public void testh3_to_geo_wkt() {
        assertNull(handler.h3_to_geo_wkt((Long)null));

        final double latitude = 50.0;
        final double longitude = -43;
        final Long h3 = handler.geo_to_h3(latitude, longitude, 4);

        assertEquals(handler.h3_to_geo_wkt(h3), "POINT (50.166306 -42.941921)");
    }

    /** Tests h3_get_resolution functions. */
    public void testh3_get_resolution() {
        final double latitude = 50.0;
        final double longitude = -43;

        assertNull(handler.h3_get_resolution((Long)null));
        assertNull(handler.h3_get_resolution((String)null));
        
        for (int i = 0; i < 16; ++i) {
            final Long h3 = handler.geo_to_h3(latitude, longitude, i);
            final Long h3Address = handler.geo_to_h3(latitude, longitude, i);
            assertEquals(i, handler.h3_get_resolution(h3));
            assertEquals(i, handler.h3_get_resolution(h3Address));
        }
    }

    public void test_h3_get_base_cell() {
        final double latitude = 40.0;
        final double longitude = -42;
        
        final Long h3 = handler.geo_to_h3(latitude, longitude, 5);
        final Long h3Address = handler.geo_to_h3(latitude, longitude, 5);

        assertNull(handler.h3_get_base_cell((Long)null));
        assertNull(handler.h3_get_base_cell((String)null));

        assertEquals(handler.h3_get_base_cell(h3), handler.h3_get_base_cell(h3Address));
        assertEquals(h3Core.h3GetBaseCell(h3), handler.h3_get_base_cell(h3Address));

        
    }

    public void teststring_to_h3_two_ways() {

        double latitude = (Math.random() * 180.0) - 90.0;
        double longitude = (Math.random() * 360.0) - 180.0;
        
        final Long h3 = handler.geo_to_h3(latitude, longitude, 5);
        final String h3Address = handler.geo_to_h3_address(latitude, longitude, 5);

        assertEquals(h3, handler.string_to_h3(h3Address));
        assertEquals(h3Address, handler.h3_to_string(h3));

        assertNull(handler.string_to_h3(null));
        assertNull(handler.h3_to_string(null));
    }


    /** Tests h3_to_geo_boundary_wkt functions  */
    public void testh3_to_geo_boundary_wkt() {
        assertNull(handler.h3_to_geo_boundary_wkt((Long)null));
        assertNull(handler.h3_to_geo_boundary_wkt((String)null));

        final double latitude = 50.0;
        final double longitude = -43;
        final Long h3 = handler.geo_to_h3(latitude, longitude, 4);
        final Long h3Address = handler.geo_to_h3(latitude, longitude, 4);

        final String[] boundaries = { "POINT (50.388228 -43.038419)", 
          "POINT (50.211244 -43.296298)", 
          "POINT (49.989297 -43.199289)", 
          "POINT (49.943733 -42.846071)", 
          "POINT (50.120176 -42.587347)", 
          "POINT (50.342723 -42.682672)"
        };

        assertEquals( Arrays.asList(boundaries), 
                      handler.h3_to_geo_boundary_wkt(h3));
        assertEquals( Arrays.asList(boundaries), 
                      handler.h3_to_geo_boundary_wkt(h3Address));

    } 

    /** Tests h3_to_geo_boundary functions as well as h3_to_geo_boundary_sys. */
    public void testh3_to_geo_boundary() {

        assertNull(handler.h3_to_geo_boundary((Long)null, ","));
        assertNull(handler.h3_to_geo_boundary((String)null, ","));
        assertNull(handler.h3_to_geo_boundary_sys((Long)null, "lng"));
        assertNull(handler.h3_to_geo_boundary_sys((String)null, "lng"));

        
        final double latitude = 50.0;
        final double longitude = -43;
        final Long h3 = handler.geo_to_h3(latitude, longitude, 4);
        final String h3Address = handler.geo_to_h3_address(latitude, longitude, 4);

        assertNull(handler.h3_to_geo_boundary_sys(h3, null));
        assertNull(handler.h3_to_geo_boundary_sys(h3Address, null));

        final List<Double> resultLat = handler.h3_to_geo_boundary_sys(h3, "lat");
        final List<Double> resultLng = handler.h3_to_geo_boundary_sys(h3, "lng");
        final List<Double> resultLatAddr = handler.h3_to_geo_boundary_sys(h3Address, "lat");
        final List<Double> resultLngAddr = handler.h3_to_geo_boundary_sys(h3Address, "lng");

        assertThrows(IllegalArgumentException.class,
                        () -> handler.h3_to_geo_boundary_sys(h3, "unk"));


        for (int i = 0; i < 6; ++i) {
            
            String[] splittedResult = handler.h3_to_geo_boundary(h3, ",")
                                             .get(i)
                                             .split(",");
            String[] splittedResultAddr = handler.h3_to_geo_boundary(h3, ",")
                                                 .get(i)
                                                 .split(",");
            
            assertEquals(h3Core.h3ToGeoBoundary(h3).get(i).lat , 
                            Double.parseDouble(splittedResult[0]), 
                            1e-4);

            assertEquals(h3Core.h3ToGeoBoundary(h3).get(i).lng , 
                            Double.parseDouble(splittedResult[1]),
                            1e-4);

            assertEquals(h3Core.h3ToGeoBoundary(h3Address).get(i).lat , 
                            Double.parseDouble(splittedResult[0]), 
                            1e-4);
            assertEquals(h3Core.h3ToGeoBoundary(h3Address).get(i).lng , 
                            Double.parseDouble(splittedResult[1]),
                            1e-4);

            assertEquals(h3Core.h3ToGeoBoundary(h3).get(i).lat, 
                            resultLat.get(i),
                            1e-4);
            assertEquals(h3Core.h3ToGeoBoundary(h3).get(i).lng,
                            resultLng.get(i),
                            1e-4);

            assertEquals(resultLatAddr.get(i), resultLat.get(i), 1e-4);
            assertEquals(resultLngAddr.get(i), resultLng.get(i), 1e-4);
        }       
    }

    public void testh3_is_valid() {
        final double latitude = (Math.random() * 180.0) - 90.0;
        final double longitude = (Math.random() * 360.0) - 180.0;
        
        final Long h3 = handler.geo_to_h3(latitude, longitude, 4);
        final String h3Address = handler.geo_to_h3_address(latitude, longitude, 4);

        assertTrue(handler.h3_is_valid(h3));
        assertTrue(handler.h3_is_valid(h3Address));
        assertFalse(handler.h3_is_valid(4L));
        assertFalse(handler.h3_is_valid("4"));
        assertFalse(handler.h3_is_valid((Long)null));
        assertFalse(handler.h3_is_valid((String)null));
    }

    public void testh3_is_res_class_iii() {
        final long h3False = 622506764662964223L;
        final long h3True = 617420388352917503L;

        assertFalse(handler.h3_is_res_class_iii(h3False));
        assertTrue(handler.h3_is_res_class_iii(h3True));

        assertFalse(handler.h3_is_res_class_iii(handler.h3_to_string(h3False)));
        assertTrue(handler.h3_is_res_class_iii(handler.h3_to_string(h3True)));

        assertFalse(handler.h3_is_res_class_iii((Long)null));
        assertFalse(handler.h3_is_res_class_iii((String)null));
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
