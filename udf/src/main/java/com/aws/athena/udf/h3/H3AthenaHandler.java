package com.aws.athena.udf.h3;


import com.amazonaws.athena.connector.lambda.handlers.UserDefinedFunctionHandler;
import com.uber.h3core.AreaUnit;
import com.uber.h3core.H3Core;
import com.uber.h3core.LengthUnit;
import com.uber.h3core.exceptions.DistanceUndefinedException;
import com.uber.h3core.exceptions.LineUndefinedException;
import com.uber.h3core.exceptions.PentagonEncounteredException;
import com.uber.h3core.util.GeoCoord;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.LinkedList;
import java.util.List;
import java.util.HashMap;
import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Collectors;


public class H3AthenaHandler extends UserDefinedFunctionHandler {

    private final H3Core h3Core;
    private static final String SOURCE_TYPE = "h3_athena_udf_handler";

    public H3AthenaHandler() throws IOException {
        super(SOURCE_TYPE);
        this.h3Core = H3Core.newInstance();
    }

    /** Indexes the location at the specified reolution, returnin index of the cell as number containing
     *  the location.
     *   @param lat the latitude of the location
     *   @param lng the longitude of the location
     *   @param res the resolution 0 &lt;= res &lt;= 15
     *   @return The H3 index as a long. Null when one of the parameter is null.
     *   @throws IllegalArgumentException latitude, longitude, or resolution are out of range.
     */
    public Long geo_to_h3(Double lat, Double lng, Integer res) {
        final Long result;
        if (lat == null || lng == null || res == null) {
            result = null;
        } else {
           result = h3Core.geoToH3(lat, lng, res);
        }
        return result;
    }

    /** Indexes the location at the specified reolution, returning index of the cell as String containing
     *  the location.
     *   @param lat the latitude of the location
     *   @param lng the longitude of the location
     *   @param res the resolution 0 &lt;= res &lt;= 15
     *   @return The H3 index as a long. Null when one of the parameter is null.
     *   @throws IllegalArgumentException latitude, longitude, or resolution are out of range.
     */
    public String geo_to_h3_address(Double lat, Double lng, Integer res) {
        final String result;
        if (lat == null || lng == null || res == null) {
            result = null;
        } else {
            result =  h3Core.geoToH3Address(lat, lng, res);
        }
        return result;
    }

    /** Finds the centroid of an index, and returns an array list of coordinates representing latitude and longitude 
     *  respectively.
     *  @param h3 the H3 index
     *  @return List of Double of size 2 representing latitude and longitude. Null when the index is null.
     *  @throws IllegalArgumentException when the index is out of range
     */
    public List<Double> h3_to_geo(Long h3) {
        final List<Double> result;
        if (h3 == null) {
            result = null;
        } else {
            final GeoCoord coord = h3Core.h3ToGeo(h3);
            result =  new ArrayList<>(Arrays. asList(coord.lat, coord.lng));
        }
	return result;
    }

    /** Finds the centroid of an index, and returns a WKT of the centroid.
     *  @param h3 the H3 index
     *  @return the WKT of the centroid of an H3 index. Null when the index is null;
     *  @throws IllegalArgumentException when the index is out of range
     */
    public String h3_to_geo_wkt(Long h3) {
        final String result;
        if (h3 == null) {
            result = null;
        } else {
          final GeoCoord coord = h3Core.h3ToGeo(h3);
          result = wktPoint(coord);
        }
        return result;        
    }

     /** Finds the centroid of an index, and returns an array list of coordinates representing latitude and longitude 
     *  respectively.
     *  @param h3Address the H3 index in its string form
     *  @return List of Double of size 2 representing latitude and longitude. Null when the address is null
     *  @throws IllegalArgumentException when the address is out of range
     */
    public List<Double> h3_to_geo(String h3Address){
        final List<Double> result;
        if (h3Address == null) {
            result = null;
        } else {
            result = pointsList(h3Core.h3ToGeo(h3Address));
        }
        return result;
    }

    /** Finds the centroid of an index, and returns a WKT of the centroid.
     *  @param h3Address the H3 index in its string form
     *  @return the WKT of the centroid of an H3 index. Null when the address is null.
     *  @throws IllegalArgumentException  when address is out of range 
     */
    public String h3_to_geo_wkt(String h3Address){
        final String result;
        if (h3Address == null) {
            result = null;
        } else {
            result = wktPoint(h3Core.h3ToGeo(h3Address));
        }
        return result;
    }

    /** Finds the boundary of an H3 index.
     * @param h3 the H3 index
     * @param sep the separator between the latitude and longitude.
     * @return the list of points representing the points in the boundary. Each returned list consists of two members, the first one is latitude, and the 
     * second one is longitude. Null when the parameter is null.
     * @throws IllegalArgumentException  when address is out of range 
     */
    public List<String> h3_to_geo_boundary(Long h3, String sep){
        final List<String> result;
        if (h3 == null) {
            result = null;
        } else {
            result =  h3Core.h3ToGeoBoundary(h3).stream()
                            .map(n-> pointsListStr(n, sep))
                            .collect(Collectors.toList()); 
        }
        return result;
    }


    /** Finds the boundary of an H3 index for a given coordinate system (lat=latitude, or lng=longitude)
     * @param h3 the H3 index
     * @param coord_sys the coordinate system, lng or lat.
     * @return the list of points representing the points in the boundary. Each returned list consists of two members, the first one is latitude, and the 
     * second one is longitude. Null when the parameter is null.
     * @throws IllegalArgumentException  when address is out of range  or when coord_sys is unknown.
     */
    public List<Double> h3_to_geo_boundary_sys(Long h3, String coord_sys) {
        final List<Double> result;
        if (coord_sys.equals("lat") || coord_sys.equals("lng")) {
            if (h3 == null) {
                result = null;
            } else {
                result =  h3Core.h3ToGeoBoundary(h3).stream()
                                .map(n-> coord_sys.equals("lat") ? n.lat : n.lng)
                                .collect(Collectors.toList()); 
            }
        } else throw new IllegalArgumentException("Unknown coord sys");
        return result;
    }


    /** Finds the boundary of an H3 index. Returns the result in an array of WKT points.
     * @param h3 the H3 index
     * @return the list of points representing the points in the boundary. Each returned list consists of a WKT representation of the point.
     * Null when h3 is null.
     * @throws IllegalArgumentException  when address is out of range.
     */
    public List<String> h3_to_geo_boundary_wkt(Long h3){
        final List<String> result;
        if (h3 == null) {
            result = null;
        } else {
            result =  h3Core.h3ToGeoBoundary(h3).stream()
                            .map(H3AthenaHandler::wktPoint)
                            .collect(Collectors.toList());
        }
        return result;        

    }

    
    /** Finds the boundary of an H3 index in a string form.
     * @param h3Address the H3 index
     * @return the list of points representing the points in the boundary. Each returned list consists of two members, the first one is latitude, and the 
     * second one is longitude . Null when the h3Address is null.
     * @throws IllegalArgumentException  when address is out of range.
     */
    public List<String> h3_to_geo_boundary_wkt(String h3Address){
        final List<String> result;
        if (h3Address == null) {
           result = null;
        } else {

           result = h3Core.h3ToGeoBoundary(h3Address).stream()
                          .map(H3AthenaHandler::wktPoint)
                          .collect(Collectors.toList());
        }
        return result;
    
    }
    
    /** Finds the boundary of an H3 index for a given coordinate system (lng=longitude, lat=latitude).
     * @param h3Address the H3 index 
     * @param coord_sys the coordinate system, lng or lat.
     * @return the list of points representing the points in the boundary. Each returned list consists of a WKT representation of the point.
     * Null when h3Address is null.
     * @throws IllegalArgumentException  when address is out of range. 
     */
    public List<Double> h3_to_geo_boundary_sys(String h3Address, String coord_sys){
        final List<Double> result;
        if (coord_sys.equals("lat") || coord_sys.equals("lng")) {

            if (h3Address == null) {
                result = null;
            }
            else { 
                result =  h3Core.h3ToGeoBoundary(h3Address).stream()
                                .map(n-> coord_sys.equals("lat") ? n.lat : n.lng)
                                .collect(Collectors.toList());
            }
        } else throw new IllegalArgumentException("Unknown coord_sys");
        return result;
    }

    /** Finds the boundary of an H3 address.
     * @param h3 the H3 index
     * @param sep the separator between the latitude and longitude.
     * @return the list of points representing the points in the boundary. Each returned list consists of two members, the first one is latitude, and the 
     * second one is longitude. Null when the parameter is null.
     * @throws IllegalArgumentException  when address is out of range 
     */
    public List<String> h3_to_geo_boundary(String h3Address, String sep){
        final List<String> result;
        if (h3Address == null) {
            result = null;
        } else {
            result =  h3Core.h3ToGeoBoundary(h3Address).stream()
                            .map(n-> pointsListStr(n, sep))
                            .collect(Collectors.toList()); 
        }
        return result;
    }



    /** Returns the resolution of an index.
     *  @param h3 the H3 index.
     *  @return the resolution. Null when h3 is null.
     *  @throws  IllegalArgumentException  when index is out of range.
     */
    public Integer h3_get_resolution(Long h3){
        final Integer result;
        if (h3 == null) {
            result = null;
        } else {
            result = h3Core.h3GetResolution(h3);
        }
        return result;
    }


    /** Returns the resolution of an index.
     *  @param h3Address the H3 index in string form.
     *  @return the resolution. Null when h3Address is null.
     */
    public Integer h3_get_resolution(String h3Address){
        final Integer result;
        if (h3Address == null) {
            result = null;
        } else {
            result =  h3Core.h3GetResolution(h3Address);
        }
        return result;
    }

    /** Returns the base cell number of the index.
     * @param h3 the index. 
     * @return the base cell number of the index. Null when h3 is null.
     */
    public Integer h3_get_base_cell(Long h3){
        final Integer result;
        if (h3 == null) {
            result = null;
        } else {
           result = h3Core.h3GetBaseCell(h3);
        }
        return result;

    }

    /** Returns the base cell number of the index in string form
     * @param h3Address the address. 
     * @return the base cell number of the index. Null when h3Address is null.
     * @throws IllegalArgumentException when index is out of range.
     */
    public Integer h3_get_base_cell(String h3Address){
        final Integer result;
        if (h3Address == null) {
            result = null;
        } else {
            result =  h3Core.h3GetBaseCell(h3Address);
        }
        return result;
    }

    /** Converts the string representation to H3Index (uint64_t) representation.
    *   @param h3Address the h3 address.
    *   @return the string representation. Null when h3Address is null.
    */
    public Long string_to_h3(String h3Address){
        final Long result;
        if (h3Address == null) {
            result = null;
        } else {
            result = h3Core.stringToH3(h3Address);
        }
        return result;
    }

    /** Converts the H3Index representation of the index to the string representation. str must be at least of length 17.
     *  @param the h3 the h3 index.
     *  @return the string representation if the index or Null when h3 is null.
     */
    public String h3_to_string(Long h3) {
        final String result;
        if (h3 == null) {
            result = null;
        } else {
            result =  h3Core.h3ToString(h3);
        }
        return result;        
    }
    
    /** Returns whether an h3 value is valid.
     *  @param h3 the h3 index.
     *  @return whether or not the index is in the range. false when h3 is null.
     */
    public Boolean h3_is_valid(Long h3) {
        return h3 != null && h3Core.h3IsValid(h3);
    }
    
    /** Returns whether an h3 address is valid.
     *  @param h3Address the h3 address to check.
     *  @return whether the h3 address is a valid h3 address. false when h3Address is null.
     */
    public Boolean h3_is_valid(String h3Address){
        return  h3Address != null && h3Core.h3IsValid(h3Address);
    }

    /** Returns whether an h3 index is ResClassIII. 
     *  @param h3 the h3 index to check.
     *  @return whether the h3 index is resClassIII. False when h3 is null.
     */
    public Boolean h3_is_res_class_iii(Long h3){
        return h3 != null && h3Core.h3IsResClassIII(h3);
    }
    
    /** Returns whether an h3 address is ResClassIII.
     * @param h3Address the h3 address to check.
     * @return whether the h3 address is resClassIII. False when h3 is null.
     */
    public Boolean h3_is_res_class_iii(String h3Address) {
        return h3Address != null &&  h3Core.h3IsResClassIII(h3Address);
    }

    /** Returns whether an H3 index is a pentagon or not.
     * @param h3 the h3 index.
     * @return whether or not the h3 item is pentagon.
     */
    public Boolean h3_is_pentagon(Long h3){
        return h3 != null && h3Core.h3IsPentagon(h3);
    }

    /** Returns whether an H3 address is a pentagon or not.
     * @param h3 the h3 address.
     * @return whether or not the h3 item is pentagon.
     */
    public Boolean h3_is_pentagon(String h3Address){
        return h3Address != null && h3Core.h3IsPentagon(h3Address);
    }

    /** Finds all icosahedron faces intersected by a given H3 index.
     *   @param h3 h3 index.
     *   @return all icosahedron faces. Null when h3 is null.
     */
    public List<Integer> h3_get_faces(Long h3){
        final List<Integer> result;  
        if (h3 == null) {
            result = null;
        } else {
            result = new ArrayList<>(h3Core.h3GetFaces(h3));
        }
        return result;
    }

    /** Find all icosahedron faces intersected by a given H3 address.
        @param h3Address the h3 address. 
        @return the list of icosahedron faces. Null when h3Address is null.
     */
    public List<Integer> h3_get_faces(String h3Address){
        final List<Integer> result;
        if (h3Address == null) {
            result = null;
        } else {
            result = new ArrayList<>(h3Core.h3GetFaces(h3Address));
        }
        return result;
    }

    /** k-rings produces indices within k distance of the origin index.
     *   @param origin the origin H3 index.
     *   @param k the distance.
     */
    public List<Long> k_ring(Long origin, Integer k){
        final List<Long> result;
        if (origin == null || k == null){
            result = null;
        } else {
            result = h3Core.kRing(origin, k);
        }
        return result;

    }


    /** k-rings produces indices within k distance of the origin H3 address.
     *   @param origin the origin H3 address.
     *   @param k the distance.
     */
    public List<String> k_ring(String origin, Integer k){
        final List<String> result;
        if (origin == null) {
            result =  null;
        } else {
            result =  h3Core.kRing(origin, k);
        }
        return result;
    }

    
    /** Produces the hollow hexagonal ring centered at origin with sides of length k.
     *  @param h3 the h3 Index.
     *  @param the length.
     */
    public List<Long> hex_ring(Long h3, Integer k) throws PentagonEncounteredException{
        final List<Long> result;
        if (h3 == null) {
            result = null;
        } else {
            result = h3Core.hexRing(h3, k);
        }
        return result;
    }

    /** Produces the hollow hexagonal ring centered at origin with sides of length k.
     *  @param h3Address the h3 Address.
     *  @param the length.
     */
    public List<String> hex_ring(String h3Address, Integer k) throws PentagonEncounteredException {
        final List<String> result;
        if (h3Address == null){
            result = null;
        } else {
            result =  h3Core.hexRing(h3Address, k);
        }
        return result;
    }

    /** Given two H3 indexes, return the line of indexes between them (inclusive).
     *   @param start the h3 index of start of the line.
     *   @param end the h3 index of end of the line.
     */
    public List<Long> h3_line(Long start, Long end)  {
        List<Long> result;
        if (start == null || end == null) {
            result =  null;
        } else {
            try{
                result = h3Core.h3Line(start, end);
            } catch (LineUndefinedException e) {
                result = null;
            }
        }
        return result;

    }

    /** Given two H3 indexes, return the line of indexes between them (inclusive).
     *   @param start the h3 address of start of the line.
     *   @param end the h3 address of end of the line.
     */
    public List<String> h3_line(String startAddress, String endAddress) throws LineUndefinedException {
        List<String> result;
        if (startAddress == null || endAddress == null) {
            result = null;
        }
        else {
            try{
                result = h3Core.h3Line(startAddress, endAddress);
            } catch (LineUndefinedException e) {
                result = null;
            }
        }
        return result;
    }

    
    public Integer h3_distance(Long a, Long b) throws DistanceUndefinedException{
        final Integer result;
        if (a == null || b == null) {
            result =  null;
        } else if (h3Core.h3GetResolution(a) == h3Core.h3GetResolution(b)) {
            result =  h3Core.h3Distance(a, b);
        } else {
            throw new IllegalArgumentException("Cannot compute distance of two indexes from different resolutions");
        } 
        return result;       
    }

    public Integer h3_distance(String a, String b) throws DistanceUndefinedException {
        final Integer result;
        if (a == null || b == null) {
            result =  null;
        } else if (h3Core.h3GetResolution(a) == h3Core.h3GetResolution(b)) {
            result = h3Core.h3Distance(a, b);
        } else {
            throw new IllegalArgumentException("Cannot compute distance of two indexes from different resolutions");
        }
        return result;
    }

    /** Returns the direct parent (parent resolution = resolution -1) index containing h.
      * @param h the h3 index.
      * @param parentRes parent resolution.
      * @return parent index containing h or null when h3 is null.
      */
    public Long h3_direct_parent(Long h) {
        final Long result;
        if (h == null) {
            result = null;
        } else {
            result =  h3Core.h3ToParent(h, h3Core.h3GetResolution(h) - 1);
        }
        return result;
    }

    /** Returns the parent (coarser) index containing h.
      * @param h3 the h3 index.
      * @param parentRes parent resolution.
      * @return parent index containing h or null when h3 is null.
      */
    public Long h3_to_parent(Long h3, Integer parentRes) {
        final Long result;
        if (h3 == null || parentRes == null) {
            result = null;
        } else {
            result =  h3Core.h3ToParent(h3, parentRes);
        }
        return result;
    }

    /** Returns the parent (coarser) index containing h3Address. */
    public String h3_to_parent(String h3Address, Integer parentRes) {
        final String result;
        if (h3Address == null){
            result = null;
        } else {
            result = h3Core.h3ToParentAddress(h3Address, parentRes);
        }
        return result;
    }

    /** Returns the direct parent (parent resolution = resolution -1) index containing h.
      * @param h the h3 index.
      * @param parentRes parent resolution.
      * @return parent adress containing h or null when h3 is null.
      */
    public String h3_direct_parent(String h) {
        return  (h == null) ? null : h3Core.h3ToParentAddress(h, h3Core.h3GetResolution(h) - 1);
    }

    /** Populates children with the indexes contained by h at resolution childRes. 
     *  @param h3 the h3 index
     *  @param childRes the children resolution
     */
    public List<Long> h3_to_children(Long h3, Integer childRes) {
        return h3 == null ? null : h3Core.h3ToChildren(h3, childRes);
    }

    /** Populates children with the indexes contained by h at resolution childRes. 
     *  @param h3 the h3 index
     *  @param childRes the children resolution
     */
    public List<String> h3_to_children(String h3Address, Integer childRes) {
        return (h3Address == null) ? null : h3Core.h3ToChildren(h3Address, childRes);
    }

    /** Returns the center child (finer) index contained by h at resolution childRes.
     * @param h3 the h3 index
     * @param childRes the child resolution
    */
    public Long h3_to_center_child(Long h3, Integer childRes){
        return (h3 == null) ? null :  h3Core.h3ToCenterChild(h3, childRes);
    }
 
    /** Returns the center child (finer) index contained by h at resolution childRes.
     * @param h3 the h3 index
     * @param childRes the child resolution
    */
    public String h3_to_center_child(String h3Address, Integer childRes){
        return (h3Address == null) ? null : h3Core.h3ToCenterChild(h3Address, childRes);
    }

    /** Compacts the set h3Set of indexes as best as possible, into the array compacted set. 
     *  This function compacts a set of cells of the same resolution into a set of cells across multiple 
     *  resolutions that represents the same area.
     */
    public List<Long> compact(List<Long> h3){
        return (h3 == null) ? null : h3Core.compact(h3);
    }


    /** Compacts the set h3Set of indexes as best as possible, into the array compacted set. 
     */
    public List<String> compact_address(List<String> h3Addresses) {
        return (h3Addresses == null) ? null : h3Core.compactAddress(h3Addresses);
    }

    /** This function uncompacts a compacted set of H3 cells to indices of the target resolution.
     *  @param h3 the list of indices, may be in different resolutions
     *  @param res the target resolution.
     *  @param list of indices in the target resolution
    */
    public List<Long> uncompact(List<Long> h3, Integer res) {
        return (h3 == null) ? null : h3Core.uncompact(h3, res);
    }
 

    /** This function uncompacts a compacted set of H3 cells to indices of the target resolution.
     *  @param h3 the list of indices, may be in different resolutions
     *  @param res the target resolution.
     *  @param list of indices in the target resolution
    */
    public List<String> uncompact_address(List<String> h3Addresses, Integer res){
        return (h3Addresses == null) ? null : h3Core.uncompactAddress(h3Addresses, res);
    }

    /** Receives a polygon WKT without holes, and resolution, and find all H3 objects whose center located inside the polygon
     *  @param polygon the polygon WKT
     *  @param res the resolution.
     *  @return H3 indexes
     */
    public List<Long> polyfill(String polygonWKT, Integer res) {
        final List<GeoCoord> geoCoordPoints = new LinkedList<>();
        final List<Long> result;

        if (polygonWKT != null && res != null) {
            final String trimmed = polygonWKT.trim();
            if (trimmed.startsWith("POLYGON ((") && trimmed.endsWith("))")) {
                
                for (String coordinates:trimmed.substring("POLYGON ((".length(), trimmed.length() - 2).split(",")) {
                    
                    final String[] splitCoordinates = coordinates.trim().split("\\s+");

                    geoCoordPoints.add(new GeoCoord(
                                            Double.parseDouble(splitCoordinates[0]),
                                            Double.parseDouble(splitCoordinates[1])));
                }
                final List<List<GeoCoord>> geoCoordHoles = new ArrayList<>();
                result = h3Core.polyfill(geoCoordPoints, geoCoordHoles, res);
            } else {
                throw new IllegalArgumentException("invalid polygonWKT");
            }
            
        }
        else {
            result = null;
        }

        return result;
    }
    
    /** Receives a polygon WKT (without holes) and returns all the H3 polygon at resolution res whose center located inside
     *  the polygon.
     *  @param polygon the WKT points of polygon.
     *  @param res the resolution.
     *  @param H3 addresses
     */
    public List<String> polyfill_address(String polygonWKT, Integer res) {
        final List<GeoCoord> geoCoordPoints = new LinkedList<>();
        final List<String> result;

        if (polygonWKT != null && res != null) {
            String trimmed = polygonWKT.trim();
            if (trimmed.startsWith("POLYGON ((") && trimmed.endsWith("))")) {
                for (String coordinates:trimmed.substring("POLYGON ((".length(), trimmed.length() - 2).split(",")) {
                    String[] splitCoordinates = coordinates.trim().split("\\s+");

                    geoCoordPoints.add(new GeoCoord(Double.parseDouble(splitCoordinates[0]), Double.parseDouble(splitCoordinates[1])));
                }
                final List<List<GeoCoord>> geoCoordHoles = new ArrayList<>();
                result = h3Core.polyfillAddress(geoCoordPoints, geoCoordHoles, res);
            } else {
                throw new IllegalArgumentException("invalid polygonWKT");
            }
            
        }
        else {
            result = null;
        }
        return result;
    }
     /** Gets a multipolygon WKT given an h3 set. 
     *  @param h3 h3 set.
     *  @param geoJson 
     */
    private String h3SetToMultiPolygon(List<Long> h3, List<String> h3Addresses, Boolean geoJson) {
        final String result;
        if (h3 == null && h3Addresses== null) {
            result =  null;
        } else {
            final List<List<List<GeoCoord>>> multiPolygon = (h3 == null) ?  
                                                                h3Core.h3AddressSetToMultiPolygon(h3Addresses, geoJson): 
                                                                h3Core.h3SetToMultiPolygon(h3, geoJson);

            final StringBuilder multiPolygonWKT = new StringBuilder("MULTIPOLYGON (");
            boolean firstPolygon = true;    
            for (List<List<GeoCoord>> polygon: multiPolygon) {
                if (firstPolygon) {
                    firstPolygon = false;
                }
                else {
                    multiPolygonWKT.append(", ");
                }
                multiPolygonWKT.append("(");

                for (List<GeoCoord> points: polygon) {
                    multiPolygonWKT.append("(");

                    boolean firstPoint = true;
                    for (GeoCoord coord: points) {
                        if (firstPoint) {
                            firstPoint = false;
                        }
                        else {
                            multiPolygonWKT.append(", ");
                        }

                        multiPolygonWKT.append(coord.lat).append(" ").append(coord.lng);
                    }
                    multiPolygonWKT.append(")");

                }
                multiPolygonWKT.append(")");
            }
            multiPolygonWKT.append(")");
            result = multiPolygonWKT.toString();
        }
        return result;
    }

    /** Gets a multipolygon WKT given an h3 set. 
     *  @param h3 h3 set.
     *  @param geoJson 
     */
    public String h3_set_to_multipolygon(List<Long> h3, Boolean geoJson) {
        return h3SetToMultiPolygon(h3, null, geoJson);
    }

    /** Gets a multipolygon WKT given an h3 set. 
     *  @param h3 h3 set.
     *  @param geoJson 
     */
    public String h3_address_set_to_multipolygon(List<String> h3Addresses, Boolean geoJson) {
        return h3SetToMultiPolygon(null, h3Addresses, geoJson);
    }


    /** Returns whether or not the provided H3Indexes are neighbors.
     *  @param origin the first h3 index
     *  @param destination the second h3 index
     *  @return true when the two h3 indexes are neighbors.
     */
    public Boolean h3_indexes_are_neighbors(Long origin, Long destination){
        final Boolean result;
        if (origin == null || destination == null ){
            result = null;
        } else {
            result =  h3Core.h3IndexesAreNeighbors(origin, destination);
        }
        return result;
    }


    /** Returns whether or not the provided H3 addresses are neighbors.
     *  @param origin the first h3 address
     *  @param destination the second h3 address
     *  @return true when the two h3 adresses are neighbors.
     */
    public Boolean h3_indexes_are_neighbors(String origin, String destination){
        final Boolean result;
        if (origin == null || destination == null ){
            result =  null;
        } else {
            result = h3Core.h3IndexesAreNeighbors(origin, destination);
        }
        return result;
    }

    /** Returns a unidirectional edge H3 index based on the provided origin and destination.
     */
    public Long get_h3_unidirectional_edge(Long origin, Long destination) {
        final Long result;
        if (origin == null || destination == null ){
            result = null;
        } else {
            result =  h3Core.getH3UnidirectionalEdge(origin, destination);
        }
        return result;
    }

    /** Returns a unidirectional edge H3 index based on the provided origin and destination.
     */
    public String get_h3_unidirectional_edge(String origin, String destination) {
        final String result;
        if (origin == null || destination == null ){
            result = null;
        } else {
            result = h3Core.getH3UnidirectionalEdge(origin, destination);
        }
        return result;
    }

    /** Determines if the provided H3Index is a valid unidirectional edge index.

     */
    public Boolean h3_unidirectional_edge_is_valid(Long edge){
        return edge != null && h3Core.h3UnidirectionalEdgeIsValid(edge);
    }


    /** Determines if the provided H3 edge address is a valid unidirectional edge index.

     */
    public Boolean h3_unidirectional_edge_is_valid(String edgeAddress){
        return edgeAddress != null && h3Core.h3UnidirectionalEdgeIsValid(edgeAddress);      
    }

    /** Returns the origin hexagon from the unidirectional edge H3Index.

    */
    public Long get_origin_h3_index_from_unidirectional_edge(Long edge){
        final Long result;
        if (edge == null) {
            result = null;
        } else {
            result = h3Core.getOriginH3IndexFromUnidirectionalEdge(edge);
        }
        return result;
    }

    /** Returns the origin hexagon from the unidirectional edge H3Index. */
    public String get_origin_h3_index_from_unidirectional_edge(String edgeAddress){
        final String result;
        if (edgeAddress == null){
            result = null;
        } else {
            result =  h3Core.getOriginH3IndexFromUnidirectionalEdge(edgeAddress);
        }
        return result;
    }

    /** Returns the destination hexagon from the unidirectional edge H3Index. */
    public Long get_destination_h3_index_from_unidirectional_edge(Long edge){
        final Long result;
        if (edge == null) {
            result = null;
        } else {
            result =  h3Core.getDestinationH3IndexFromUnidirectionalEdge(edge);
        }
        return result;
    }
    /** Returns the destination hexagon from the unidirectional edge address. */
    public String get_destination_h3_index_from_unidirectional_edge(String edgeAddress){
        final String result;
        if (edgeAddress == null){
            result = null;
        } else {
            result =  h3Core.getDestinationH3IndexFromUnidirectionalEdge(edgeAddress);
        }
        return result;
    }

    /** Provides all of the unidirectional edges from the current H3Index. */
    public List<Long> get_h3_unidirectional_edges_from_hexagon(Long h3){
        final List<Long> result;
        if (h3 == null) {
            result = null;
        } else {
            result =  h3Core.getH3UnidirectionalEdgesFromHexagon(h3);
        }
        return result;
    }
    

    /** Provides all of the unidirectional edges from the current H3 address. */
    public List<String> get_h3_unidirectional_edges_from_hexagon(String h3){
        final List<String> result;
        if (h3 == null) {
            result = null;
        } else {
            result =  h3Core.getH3UnidirectionalEdgesFromHexagon(h3);
        }
        return result;
    }


    /** Provides all of the unidirectional edges from the current H3Index. 
     *  It returns the WKT Points.
     */
    public List<String> get_h3_unidirectional_edge_boundary(Long edge){
        final List<String> result;
        if (edge == null) {
            result = null;
        } else {
            result = h3Core.getH3UnidirectionalEdgeBoundary(edge).stream()
                           .map(H3AthenaHandler::wktPoint)
                           .collect(Collectors.toList());
        }
        return result;
    }

    /** Provides all of the unidirectional edges from the current H3Index. 
     *  It returns the WKT Points.
     */
    public List<String> get_h3_unidirectional_edge_boundary(String edgeAddress){
        final List<String> result;
        if (edgeAddress == null) {
            result = null;
        } else{
            result = h3Core.getH3UnidirectionalEdgeBoundary(edgeAddress).stream()
                           .map(H3AthenaHandler::wktPoint)
                           .collect(Collectors.toList());
        }
        return result;
    }


    /**Average hexagon area in square kilometers at the given resolution. */
    public Double hex_area(Integer res, String unit) {
        return h3Core.hexArea(res, AreaUnit.valueOf(unit));
    }


    /** Exact area of specific cell */
    public Double cell_area(Long h3, String unit) {
        final Double result;
        if (h3 == null) {
            result = null;
        } else {
            result =  h3Core.cellArea(h3, AreaUnit.valueOf(unit));
        }
        return result;
    }

    public Double cell_area(String h3Address, String unit) {
        final Double result;
        if (h3Address == null) {
            result =  null;
        } else {
            result =  h3Core.cellArea(h3Address, AreaUnit.valueOf(unit));
        }
        return result;
    }

    /** Average hexagon edge length  at the given resolution. */
    public double edge_length(int res, String unit){
        return h3Core.edgeLength(res, LengthUnit.valueOf(unit));
    }

    /** Exact edge length of specific unidirectional edge in kilometers. */
    public Double exact_edge_length(Long h3, String unit){
        final Double result;
        if (h3 == null){
            result = null;
        } else {

            result =  h3Core.exactEdgeLength(h3, LengthUnit.valueOf(unit));
        }
        return result;
    }

    /** Returns the total count of hexagons in the world at a given resolution. 
     * @param res the resolution.
     * @return the number of hexagons at a given resolution.
     */
    public Long num_hexagons(Integer res){
        return h3Core.numHexagons(res);
    }

    public List<Long> getRes0Indexes(int res){
        return new ArrayList<Long>(h3Core.getRes0Indexes());
    }
    public List<String> getRes0IndexesAddresses(int res){
        return new ArrayList<String>(h3Core.getRes0IndexesAddresses());
    }

    /** Gets the pentagon indexes at a given resolution. 
     * @param res resolution.
     * @return the indexes of pentagons in H3 system. 
     */
    public List<Long> get_pentagon_indexes(Integer res){
        return res == null ? null : new ArrayList<Long>(h3Core.getPentagonIndexes(res));
    }

    /** Gets all pentagon addresses at a given resolution. 
     * @param res resolution.
     * @return the addresses of pentagons in H3 system. 
     */
    public List<String> get_pentagon_adresses(Integer res){
        return res == null ? null : new ArrayList<String>(h3Core.getPentagonIndexesAddresses(res));
    }

    public double pointDist(String point1, String point2, String unit){
        return h3Core.pointDist(geoCoordFromWKTPoint(point1), geoCoordFromWKTPoint(point2), LengthUnit.valueOf(unit));
    }

    private static String pointsListStr(GeoCoord geoCoord, String sep) {
        return String.format("%f%s%f", geoCoord.lng, sep, geoCoord.lat);
    }


    private static List<Double> pointsList(GeoCoord geoCoord) {
        return new ArrayList<Double>(Arrays. asList(geoCoord.lat, geoCoord.lng));
    }

    private static String wktPoint(GeoCoord geoCoord) {
        return String.format("POINT (%f %f)", geoCoord.lng, geoCoord.lat);
    }

    private static GeoCoord geoCoordFromWKTPoint(String wktPoint) {
        
        final String trimmed = wktPoint.trim();
        if (trimmed.startsWith("POINT")) {
            final String inParentheses = trimmed.substring(5, trimmed.length());
            if ( inParentheses.charAt(0) == '(' && inParentheses.charAt(inParentheses.length()-1) == ')' ){
                final String[] splitted = inParentheses.substring(1, inParentheses.length()-1).split("\\s+");
                return new GeoCoord(Double.parseDouble(splitted[0]), Double.parseDouble(splitted[1]));
            }
            else {
                throw new IllegalArgumentException("Cannot find parentheses in String" + wktPoint);
            }
        }
        else {
            throw new IllegalArgumentException("Cannot find POINT" + wktPoint);
        }
    }
}
