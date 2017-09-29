package com.example.bar;

import static org.junit.Assert.*;

import java.io.IOException;

import org.junit.Test;

import com.example.util.Opener;

import be.ceau.chart.BarChart;
import be.ceau.chart.color.Color;
import be.ceau.chart.data.BarData;
import be.ceau.chart.dataset.BarDataset;
import be.ceau.chart.options.BarOptions;

public class BarChartTest {

	@Test
	public void test() throws IOException {
		
		BarChart bar = new BarChart();
		
		BarData data = new BarData();
		BarOptions options = new BarOptions();
		
		bar.setData(data);
		bar.setOptions(options);
		
		///////////////////////////////////////
		// Data Set
		///////////////////////////////////////
		BarDataset set1 = new BarDataset();
		set1.setData(200, 500, 700, 550, 440);		//1월~5월까지 지연 건수
		set1.setLabel("1987");
		set1.setBackgroundColor(Color.AQUA);
		
		BarDataset set2 = new BarDataset();
		set2.setData(500, 1500, 900, 450, 340);		//1월~5월까지 지연 건수
		set2.setLabel("1988");
		set2.setBackgroundColor(Color.LAVENDER);
		
		BarDataset set3 = new BarDataset();
		set3.setData(1000, 780, 150, 470, 800);		//1월~5월까지 지연 건수
		set3.setLabel("1989");
		set3.setBackgroundColor(Color.SALMON);
		
		data.addDataset(set1);						//데이터 집어넣기
		data.addDataset(set2);
		data.addDataset(set3);
		data.setLabels("1M", "2M", "3M", "4M", "5M");	//라벨
		
		Opener.inBrowser(bar);
		
	}

}
