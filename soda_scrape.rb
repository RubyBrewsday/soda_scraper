require 'soda/client'
require 'pry'
require "firebase"
require "twitter"
require "json"
require "firebase_token_generator"
SECRET = "rLufLfDQuw6B6AvtmpMyjinY0EbskPPuJJylDfaq"
base_uri = 'https://foodtrucks.firebaseio.com/'
options = {:admin => true}
auth_data = {:uid => 'admin', :is_admin => 'true'}

generator = Firebase::FirebaseTokenGenerator.new(SECRET)
token = generator.create_token(auth_data, options)
$firebase = Firebase::Client.new(base_uri,token)

class SodaScrape
  attr_accessor :start_date
  def initialize(app_token="bpCgHGOPnykDpsuzihd0trChF",domain = "data.cityofnewyork.us",dataset="s4h4-j5ma")
    @client = SODA::Client.new(:domain=>domain,:app_token=>app_token)
    @dataset = "s4h4-j5ma"
    @start_date = DateTime.parse('2010-01-01').to_date
  end

  def format_ticket(ticket)
    formatted_ticket = {}
    ticket.to_hash.to_a.each do |key, value|
      formatted_ticket[key.to_sym] = value
    end
    #set priority in ms since epocy
    formatted_ticket[:".priority"] = (DateTime.parse(formatted_ticket[:violation_date]).to_time.to_i*1000)
    #convenient ID
    formatted_ticket[:id] = formatted_ticket[:ticket_number].to_i
    formatted_ticket[:dataset] = @dataset
    formatted_ticket[:created_at] = Time.now
    formatted_ticket
  end

  def add_ticket_to_firebase(ticket)
    formatted_ticket = format_ticket(ticket)
    puts $firebase.set("tickets_raw/#{formatted_ticket[:id].to_s}",formatted_ticket)
  end

  def get_tickets
    error_count = 0
    tickets = []
    i = 0

    while i < 1000000
      begin
        puts "Requesting batch #{i.to_s}"
        #"$limit"=>1000*i+999,
        response = @client.get(@dataset,{"$offset"=>1000*i, "$where"=>"violation_date >= '#{@start_date.to_s}T00:00:00'"})
        puts response.inspect
        if response.size == 0
          error_count += 1
          if error_count > 3
            return tickets
          end
        end
        puts "Received batch #{i.to_s} - #{response.length.to_s} records"
        response.each do |ticket|
          puts ticket["charge_1_code_description"]
          add_ticket_to_firebase(ticket)
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

s = SodaScrape.new
s.get_tickets
