PieChart = (function() {
  function PieChart(target) {
    var width = 250,
        height = 250,
        radius = Math.min(width, height) / 2;

    var color = d3.scale.category10()

    var arc = d3.svg.arc()
        .outerRadius(radius - 10)
        .innerRadius(0);

    var pie = d3.layout.pie()
        .sort(null)
        .value(function(d) { return d.count; });

    var svg = d3.select(target).append("svg")
        .attr("width", width)
        .attr("height", height)
        .append("g")
        .attr("transform", "translate(" + width / 2 + "," + height / 2 + ")");

    var data = [{type: "Metadaten unversehrt", count: nimbus.workingResources},
                {type: "Metadaten beschädigt", count: nimbus.workingResources - nimbus.totalResources}]

    var g = svg.selectAll(".arc")
        .data(pie(data))
        .enter().append("g")
        .attr("class", "arc");

    g.append("path")
     .attr("d", arc)
     .style("fill", function(d) { return color(d.data.type); });

    var legend = d3.select(target).append("svg")
      .attr("class", "legend")
      .attr("width", radius + 100)
      .attr("height", 50)
      .selectAll("g")
      .data(data)
      .enter().append("g")
      .attr("transform", function(d, i) { return "translate(40," + i * 20 + ")"; });

    legend.append("rect")
      .attr("width", 18)
      .attr("height", 18)
      .style("fill", function(d) { return color(d.type); });

    legend.append("text")
      .attr("x", 24)
      .attr("y", 9)
      .attr("dy", ".35em")
      .text(function(d) { return d.type; });
  }

  return PieChart

})();

$(document).ready(function() {
  pieChart = new PieChart("#linkchecker-pie-chart");
});
