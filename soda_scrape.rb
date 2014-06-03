require 'soda/client'

class SodaScrape
	
	def initialize(app_token="bpCgHGOPnykDpsuzihd0trChF",domain = "data.cityofnewyork.us") 
        @client = SODA::Client.new(:domain=>domain,:app_token=>app_token)
	end

	def get_tickets(dataset="s4h4-j5ma")
		error_count = 0
		tickets = []
		i = 0
	
		while i < 1000000
			begin
			    puts "Requesting batch #{i.to_s}"
			    #"$limit"=>1000*i+999,				
				response = @client.get(dataset,{"$offset"=>1000*i, "$where"=>"violation_date >= '2014-01-01T00:00:00'"})
				puts response.inspect
				if response.size == 0
					error_count += 1
					if error_count > 3
						return tickets
					end
				end
				puts "Received batch #{i.to_s} - #{response.length.to_s} records"
				response.each do |ticket|
					puts ticket
					tickets.push(ticket)
				end
			rescue  => e
				puts e.message
				puts e.backtrace.inspect
			end
			i += 1
		end
		
		return tickets
	end

end
