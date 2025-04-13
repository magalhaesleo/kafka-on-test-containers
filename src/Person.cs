using Avro;
using Avro.Specific;

namespace kafka_test_containers;

public class Person : ISpecificRecord
{
    public static Schema _SCHEMA = Schema.Parse("""
                                                     {
                                                         "type": "record",
                                                         "name": "User",
                                                         "fields": [
                                                             { "name": "name", "type": "string" },
                                                             { "name": "favorite_number",  "type": "long" },
                                                             { "name": "favorite_color", "type": "string" }
                                                         ]
                                                     }
                                                     """);

    public Schema Schema => _SCHEMA;

    public string Name { get; private set; }

    public long FavoriteNumber { get; private set; }

    public string FavoriteColor { get; private set; }

    public Person()
    {
        Name = string.Empty;
        FavoriteNumber = 0;
        FavoriteColor = string.Empty;
    }

    public Person(string name, long favoriteNumber, string favoriteColor)
    {
        Name = name;
        FavoriteNumber = favoriteNumber;
        FavoriteColor = favoriteColor;
    }

    public object Get(int fieldPos)
    {
        return fieldPos switch
        {
            0 => Name,
            1 => FavoriteNumber,
            2 => FavoriteColor,
            _ => throw new AvroRuntimeException("Bad index " + fieldPos + " in Get()")
        };
    }

    public void Put(int fieldPos, object fieldValue)
    {
        switch (fieldPos)
        {
            case 0: Name = (string)fieldValue; break;
            case 1: FavoriteNumber = (long)fieldValue; break;
            case 2: FavoriteColor = (string)fieldValue; break;
            default: throw new AvroRuntimeException("Bad index " + fieldPos + " in Put()");
        }
    }
}