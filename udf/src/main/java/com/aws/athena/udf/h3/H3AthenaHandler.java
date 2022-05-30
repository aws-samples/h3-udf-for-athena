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

import java.util.List;
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
    public Long geoToH3(Double lat, Double lng, Integer res) {
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
    public String geoToH3Address(Double lat, Double lng, Integer res) {
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
    public List<Double> h3ToGeo(Long h3) {
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
    public String h3ToGeoWKT(Long h3) {
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
    public List<Double> h3ToGeo(String h3Address){
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
    public String h3ToGeoWKT(String h3Address){
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
     * @return the list of points representing the points in the boundary. Each returned list consists of two members, the first one is latitude, and the 
     * second one is longitude. Null when the parameter is null.
     * @throws IllegalArgumentException  when address is out of range 
     */
    public List<List<Double>> h3ToGeoBoundary(Long h3){
        final List<List<Double>> result;
        if (h3 == null) {
            result = null;
        } else {
            result =  h3Core.h3ToGeoBoundary(h3).stream()
                            .map(H3AthenaHandler::pointsList)
                            .collect(Collectors.toList()); 
        }
        return result;
    }

    /** Finds the boundary of an H3 index.
     * @param h3 the H3 index
     * @return the list of points representing the points in the boundary. Each returned list consists of a WKT representation of the point.
     * Null when h3 is null.
     * @throws IllegalArgumentException  when address is out of range.
     */
    public List<String> h3ToGeoBoundaryWKT(Long h3){
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
    public List<String> h3ToGeoBoundaryWKT(String h3Address){
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
    
    /** Finds the boundary of an H3 index in a string form
     * @param h3Address the H3 index 
     * @return the list of points representing the points in the boundary. Each returned list consists of a WKT representation of the point.
     * Null when h3Address is null.
     * @throws IllegalArgumentException  when address is out of range. 
     */
    public List<List<Double>> h3ToGeoBoundary(String h3Address){
        final List<List<Double>> result;
        if (h3Address == null) {
            result = null;
        }
        else { 
            result =  h3Core.h3ToGeoBoundary(h3Address).stream()
                            .map(H3AthenaHandler::pointsList)
                            .collect(Collectors.toList());
        }
        return result;
    }

    /** Returns the resolution of an index.
     *  @param h3 the H3 index.
     *  @return the resolution. Null when h3 is null.
     *  @throws  IllegalArgumentException  when index is out of range.
     */
    public Integer h3GetResolution(Long h3){
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
    public Integer h3GetResolution(String h3Address){
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
    public Integer h3GetBaseCell(Long h3){
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
    public Integer h3GetBaseCell(String h3Address){
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
    public Long stringToH3(String h3Address){
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
    public String h3ToString(Long h3) {
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
     *  @return whether or not the index is in the range. False when h3 is null.
     */
    public boolean h3IsValid(Long h3) {
        return h3 != null && h3Core.h3IsValid(h3);
    }
    
    /** Returns whether an h3 address is valid.
     *  @param h3Address the h3 address to check.
     *  @return whether the h3 address is a valid h3 address. False when h3Address is null.
     */
    public boolean h3IsValid(String h3Address){
        return  h3Address != null && h3Core.h3IsValid(h3Address);
    }

    /** Returns whether an h3 index is ResClassIII. 
     *  @param h3 the h3 index to check.
     *  @return whether the h3 index is resClassIII. False when h3 is null.
    public boolean h3IsResClassIII(Long h3){
        return h3 != null && h3Core.h3IsResClassIII(h3);
    }
    
    /** Returns whether an h3 address is ResClassIII.
     * @param h3Address the h3 address to check.
     * @return whether the h3 address is resClassIII. False when h3 is null.
     */
    public boolean h3IsResClassIII(String h3Address) {
        return h3Address != null &&  h3Core.h3IsResClassIII(h3Address);
    }

    public boolean h3IsPentagon(Long h3){
        return h3 != null && h3Core.h3IsPentagon(h3);
    }
    public boolean h3IsPentagon(String h3Address){
        return h3Address != null && h3Core.h3IsPentagon(h3Address);
    }


    /** Finds all icosahedron faces intersected by a given H3 index.
        @param h3 h3 index.
        @return all icosahedron faces. Null when h3 is null.
     */
    public List<Integer> h3GetFaces(Long h3){
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
    public List<Integer> h3GetFaces(String h3Address){
        final List<Integer> result;
        if (h3Address == null) {
            result = null;
        } else {
            result = new ArrayList<>(h3Core.h3GetFaces(h3Address));
        }
        return result;
    }

    public List<Long> kRing(Long origin, int k){
        final List<Long> result;
        if (origin == null){
            result = null;
        } else {
            result = h3Core.kRing(origin, k);
        }
        return result;

    }

    public List<String> kRing(String origin, int k){
        final List<String> result;
        if (origin == null) {
            result =  null;
        } else {
            result =  h3Core.kRing(origin, k);
        }
        return result;
    }

    public List<List<Long>> kRingDistances(Long origin, int k) {
        final List<List<Long>> result;
        if (origin == null){
            result = null;
        } else {
            result =  h3Core.kRingDistances(origin, k);
        }
        return result;

    }
    public List<List<String>> kRingDistances(String origin, int k){
        final List<List<String>> result;
        if (origin == null){
            result = null;
        } else {
            result =  h3Core.kRingDistances(origin, k);
        }
        return result;
    }

    public List<List<Long>> hexRange(Long h3, int k) throws PentagonEncounteredException{
        final List<List<Long>> result;
        if (h3 == null){
            result = null;
        } else {
            result =  h3Core.hexRange(h3, k);
        }
        return result;
    }

    public List<List<String>> hexRange(String h3Address, int k) throws PentagonEncounteredException{
        final List<List<String>> result;
        if (h3Address == null){
            result = null;
        } else { 
            result = h3Core.hexRange(h3Address, k);
        }
        return result;
    }


    public List<Long> hexRing(Long h3, int k) throws PentagonEncounteredException{
        final List<Long> result;
        if (h3 == null) {
            result = null;
        } else {
            result = h3Core.hexRing(h3, k);
        }
        return result;
    }

    public List<String> hexRing(String h3Address, int k) throws PentagonEncounteredException {
        final List<String> result;
        if (h3Address == null){
            result = null;
        } else {
            result =  h3Core.hexRing(h3Address, k);
        }
        return result;
    }


    public List<Long> h3Line(Long start, Long end)  {
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
    public List<String> h3Line(String startAddress, String endAddress) throws LineUndefinedException {
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

    
    public Integer h3Distance(Long a, Long b) throws DistanceUndefinedException{
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

    public Integer h3Distance(String a, String b) throws DistanceUndefinedException {
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

    /** Returns the parent (coarser) index containing h.
      * @param h3 the h3 index.
      * @param parentRes parent resolution.
      * @return parent index containing h or null when h3 is null.
      */
    public Long h3ToParent(Long h3, int parentRes) {
        final Long result;
        if (h3 == null) {
            result = null;
        } else {
            result =  h3Core.h3ToParent(h3, parentRes);
        }
        return result;
    }

    /** Returns the parent (coarser) index containing h3Address. */
    public String h3ToParent(String h3Address, int parentRes) {
        final String result;
        if (h3Address == null){
            result = null;
        } else {
            result = h3Core.h3ToParentAddress(h3Address, parentRes);
        }
        return result;
    }

    public List<Long> h3ToChildren(Long h3, int childRes) {
        final List<Long> result;
        if (h3 == null){
            result = null;
        } else {
            result =  h3Core.h3ToChildren(h3, childRes);
        }
        return result;

    }
    public List<String> h3ToChildren(String h3Address, int childRes) {
        final List<String> result;
        if (h3Address == null) {
            result = null;
        } else {
            result = h3Core.h3ToChildren(h3Address, childRes);
        }
        return result;
    }

    public Long h3ToCenterChild(Long h3, int childRes){
        final Long result;
        if (h3 == null){
            result = null;
        } else {
            result =  h3Core.h3ToCenterChild(h3, childRes);
        }
        return result;
    }
    public String h3ToCenterChild(String h3Address, int childRes){
        final String result;
        if (h3Address == null)  {
            result = null;
        } else {
            result =  h3Core.h3ToCenterChild(h3Address, childRes);
        }
        return result;
    }

    public List<Long> compact(List<Long> h3){
        final List<Long> result;
        if (h3 == null) {
            result = null;
        } else {
            result =  h3Core.compact(h3);
        }
        return result;
    }
    public List<String> compactAddress(List<String> h3Addresses) {
        final List<String> result;
        if (h3Addresses == null) {
            result = null;
        } else {
            result = h3Core.compactAddress(h3Addresses);
        }
        return result;
    }

    public List<Long> uncompact(List<Long> h3, int res) {
        final List<Long> result;
        if (h3 == null) {
            result = null;
        } else {
            result = h3Core.uncompact(h3, res);
        }
        return result;
    }
    public List<String> uncompactAddress(List<String> h3Addresses, int res){
        final List<String> result;
        if (h3Addresses == null) {
            result = null;
        } else {
            result = h3Core.uncompactAddress(h3Addresses, res);
        }
        return result;
    }

    public List<Long> polyfill(List<String> points, List<List<String>> holes, int res) {
        final List<Long> result;
        if (points == null) {
            result = null;
        } else {
            final List<List<String>> holesList;

            if (holes == null) {
                holesList = new ArrayList<>();
            } else {
                holesList = holes;
            }

            final List<GeoCoord> geoCoordPoints =
                points.stream().map(H3AthenaHandler::geoCoordFromWKTPoint).collect(Collectors.toList());
            final List<List<GeoCoord>> geoCoordHoles =
                holesList.stream()
                        .map(x ->
                                x.stream()
                                .map(H3AthenaHandler::geoCoordFromWKTPoint)
                                .collect(Collectors.toList()))
                        .collect(Collectors.toList());
            result =  h3Core.polyfill(geoCoordPoints, geoCoordHoles, res);
        }
        return result;


    }
    
    public List<String> polyfillAddress(List<String> points, List<List<String>> holes, int res) {
        final List<String> result;
        if (points == null) {
            result = null;
        } else {

	    final List<List<String>> holesList;
            if (holes == null) {
                holesList = new ArrayList<>();
            } else {
                holesList = holes;
            }

            final List<GeoCoord> geoCoordPoints =
                points.stream().map(H3AthenaHandler::geoCoordFromWKTPoint).collect(Collectors.toList());
            final List<List<GeoCoord>> geoCoordHoles =
                holesList.stream()
                        .map(x ->
                                x.stream()
                                .map(H3AthenaHandler::geoCoordFromWKTPoint)
                                .collect(Collectors.toList()))
                        .collect(Collectors.toList());
            result =  h3Core.polyfillAddress(geoCoordPoints, geoCoordHoles, res);
        }
        return result;
    }

    public List<List<List<String>>> h3SetToMultiPolygon(List<Long> h3, boolean geoJson) {
        final List<List<List<String>>> result;
        if (h3 == null) {
            result =  null;
        } else {
            final List<List<List<GeoCoord>>> multiPolygon = h3Core.h3SetToMultiPolygon(h3, geoJson);
            result = multiPolygon.stream().map(r -> 
                            r.stream().map( polygon -> polygon.stream().map(H3AthenaHandler::wktPoint).
                                                collect(Collectors.toList()))
                                    .collect(Collectors.toList()))
                            .collect(Collectors.toList());
        }
        return result;
    }

    public List<List<List<String>>> h3AddressSetToMultiPolygon(List<String> h3Addresses, boolean geoJson){
        final List<List<List<String>>> polygons;
        if (h3Addresses == null){
            polygons = null;
        } else {
            final List<List<List<GeoCoord>>> result = h3Core.h3AddressSetToMultiPolygon(h3Addresses, geoJson);
            polygons =  result.stream().map(r -> 
                            r.stream().map( polygon -> polygon.stream().map(H3AthenaHandler::wktPoint).
                                                collect(Collectors.toList()))
                                    .collect(Collectors.toList()))
                            .collect(Collectors.toList());
        }
        return polygons;
    }

    public Boolean h3IndexesAreNeighbors(Long origin, Long destination){
        final Boolean result;
        if (origin == null || destination == null ){
            result = null;
        } else {
            result =  h3Core.h3IndexesAreNeighbors(origin, destination);
        }
        return result;
    }


    public Boolean h3IndexesAreNeighbors(String origin, String destination){
        final Boolean result;
        if (origin == null || destination == null ){
            result =  null;
        } else {
            result = h3Core.h3IndexesAreNeighbors(origin, destination);
        }
        return result;
    }

    public Long getH3UnidirectionalEdge(Long origin, Long destination) {
        final Long result;
        if (origin == null || destination == null ){
            result = null;
        } else {
            result =  h3Core.getH3UnidirectionalEdge(origin, destination);
        }
        return result;
    }
    public String getH3UnidirectionalEdge(String origin, String destination) {
        final String result;
        if (origin == null || destination == null ){
            result = null;
        } else {
            result = h3Core.getH3UnidirectionalEdge(origin, destination);
        }
        return result;
    }

    public boolean h3UnidirectionalEdgeIsValid(Long edge){
        return edge != null && h3Core.h3UnidirectionalEdgeIsValid(edge);
    }
    public boolean h3UnidirectionalEdgeIsValid(String edgeAddress){
        return edgeAddress != null && h3Core.h3UnidirectionalEdgeIsValid(edgeAddress);      
    }
    public Long getOriginH3IndexFromUnidirectionalEdge(Long edge){
        final Long result;
        if (edge == null) {
            result = null;
        } else {
            result = h3Core.getOriginH3IndexFromUnidirectionalEdge(edge);
        }
        return result;
    }
    public String getOriginH3IndexFromUnidirectionalEdge(String edgeAddress){
        final String result;
        if (edgeAddress == null){
            result = null;
        } else {
            result =  h3Core.getOriginH3IndexFromUnidirectionalEdge(edgeAddress);
        }
        return result;
    }

    public Long getDestinationH3IndexFromUnidirectionalEdge(Long edge){
        final Long result;
        if (edge == null) {
            result = null;
        } else {
            result =  h3Core.getDestinationH3IndexFromUnidirectionalEdge(edge);
        }
        return result;
    }
    public String getDestinationH3IndexFromUnidirectionalEdge(String edgeAddress){
        final String result;
        if (edgeAddress == null){
            result = null;
        } else {
            result =  h3Core.getDestinationH3IndexFromUnidirectionalEdge(edgeAddress);
        }
        return result;
    }

    public List<Long> getH3UnidirectionalEdgesFromHexagon(Long h3){
        final List<Long> result;
        if (h3 == null) {
            result = null;
        } else {
            result =  h3Core.getH3UnidirectionalEdgesFromHexagon(h3);
        }
        return result;
    }
    
    public List<String> getH3UnidirectionalEdgesFromHexagon(String h3){
        final List<String> result;
        if (h3 == null) {
            result = null;
        } else {
            result =  h3Core.getH3UnidirectionalEdgesFromHexagon(h3);
        }
        return result;
    }

    public List<String> getH3UnidirectionalEdgeBoundary(Long edge){
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
    public List<String> getH3UnidirectionalEdgeBoundary(String edgeAddress){
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

    public Double hexArea(int res, String unit) {
        return h3Core.hexArea(res, AreaUnit.valueOf(unit));
    }

    public Double cellArea(Long h3, String unit) {
        final Double result;
        if (h3 == null) {
            result = null;
        } else {
            result =  h3Core.cellArea(h3, AreaUnit.valueOf(unit));
        }
        return result;
    }

    public Double cellArea(String h3Address, String unit) {
        final Double result;
        if (h3Address == null) {
            result =  null;
        } else {
            result =  h3Core.cellArea(h3Address, AreaUnit.valueOf(unit));
        }
        return result;
    }


    public double edgeLength(int res, String unit){
        return h3Core.edgeLength(res, LengthUnit.valueOf(unit));
    }

    public Double exactEdgeLength(Long h3, String unit){
        final Double result;
        if (h3 == null){
            result = null;
        } else {

            result =  h3Core.exactEdgeLength(h3, LengthUnit.valueOf(unit));
        }
        return result;
    }

    public long numHexagons(int res){
        return h3Core.numHexagons(res);
    }

    public List<Long> getRes0Indexes(int res){
        return new ArrayList<Long>(h3Core.getRes0Indexes());
    }
    public List<String> getRes0IndexesAddresses(int res){
        return new ArrayList<String>(h3Core.getRes0IndexesAddresses());
    }

    public List<Long> getPentagonIndexes(int res){
        return new ArrayList<Long>(h3Core.getPentagonIndexes(res));
    }
    public List<String> getPentagonIndexesAddresses(int res){
        return new ArrayList<String>(h3Core.getPentagonIndexesAddresses(res));
    }

    public double pointDist(String point1, String point2, String unit){
        return h3Core.pointDist(geoCoordFromWKTPoint(point1), geoCoordFromWKTPoint(point2), LengthUnit.valueOf(unit));
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
