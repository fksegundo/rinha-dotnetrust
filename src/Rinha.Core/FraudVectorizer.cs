using System.Buffers.Text;

namespace Rinha.Core;

public static class FraudVectorizer
{
    public static bool TryVectorize(ReadOnlySpan<byte> json, Span<short> destination)
    {
        destination.Clear();

        try
        {
            if (TryVectorizeOrdered(json, destination))
                return true;

            destination.Clear();
            if (TryVectorizeUnordered(json, destination))
                return true;

            destination.Clear();
            return false;
        }
        catch
        {
            destination.Clear();
            return false;
        }
    }

    private static bool TryVectorizeOrdered(ReadOnlySpan<byte> json, Span<short> destination)
    {
        int cursor = 0;
        Span<ulong> knownHashes = stackalloc ulong[64];
        int knownCount = 0;

        if (!TryReadDouble(json, "\"amount\""u8, ref cursor, out double amount))
            return false;
        destination[0] = VectorSpec.QuantizeFlat(amount / 10_000.0);

        if (!TryReadInt(json, "\"installments\""u8, ref cursor, out int installments))
            return false;
        destination[1] = VectorSpec.QuantizeFlat(installments / 12.0);

        if (!TryReadString(json, "\"requested_at\""u8, ref cursor, out ReadOnlySpan<byte> requestedAt))
            return false;
        ParsedDateTime requestedDate = ParseDateTime(requestedAt);
        long requestedMinute = requestedDate.EpochMinute;
        destination[3] = VectorSpec.QuantizeFlat(requestedDate.Hour / 23.0);
        destination[4] = VectorSpec.QuantizeFlat(requestedDate.DayOfWeekMondayZero / 6.0);

        if (!TryReadDouble(json, "\"avg_amount\""u8, ref cursor, out double customerAvgAmount))
            return false;

        if (!TryReadInt(json, "\"tx_count_24h\""u8, ref cursor, out int txCount24h))
            return false;
        destination[8] = VectorSpec.QuantizeFlat(txCount24h / 20.0);

        if (!TryReadKnownMerchants(json, ref cursor, knownHashes, out knownCount))
            return false;

        if (!TryReadString(json, "\"id\""u8, ref cursor, out ReadOnlySpan<byte> merchantId))
            return false;
        ulong merchantHash = Hash(merchantId);

        if (!TryReadString(json, "\"mcc\""u8, ref cursor, out ReadOnlySpan<byte> mcc))
            return false;
        destination[12] = VectorSpec.QuantizeFlat(MccRisk(ParseMcc(mcc)));

        if (!TryReadDouble(json, "\"avg_amount\""u8, ref cursor, out double merchantAvgAmount))
            return false;
        destination[13] = VectorSpec.QuantizeFlat(merchantAvgAmount / 10_000.0);

        if (!TryReadBool(json, "\"is_online\""u8, ref cursor, out bool isOnline))
            return false;
        destination[9] = isOnline ? (short)VectorSpec.FlatScale : (short)0;

        if (!TryReadBool(json, "\"card_present\""u8, ref cursor, out bool cardPresent))
            return false;
        destination[10] = cardPresent ? (short)VectorSpec.FlatScale : (short)0;

        if (!TryReadDouble(json, "\"km_from_home\""u8, ref cursor, out double kmFromHome))
            return false;
        destination[7] = VectorSpec.QuantizeFlat(kmFromHome / 1_000.0);

        if (!TryFindValue(json, "\"last_transaction\""u8, ref cursor, out int lastValue))
            return false;

        if (lastValue < json.Length && json[lastValue] == (byte)'n')
        {
            destination[5] = (short)-VectorSpec.FlatScale;
            destination[6] = (short)-VectorSpec.FlatScale;
        }
        else
        {
            cursor = lastValue;
            if (!TryReadString(json, "\"timestamp\""u8, ref cursor, out ReadOnlySpan<byte> lastTimestamp))
                return false;
            if (!TryReadDouble(json, "\"km_from_current\""u8, ref cursor, out double lastKm))
                return false;

            long lastMinute = ParseDateTime(lastTimestamp).EpochMinute;
            destination[5] = VectorSpec.QuantizeFlat(Math.Max(0, requestedMinute - lastMinute) / 1_440.0);
            destination[6] = VectorSpec.QuantizeFlat(lastKm / 1_000.0);
        }

        FinishVector(destination, amount, customerAvgAmount, merchantHash, knownHashes[..knownCount]);
        return true;
    }

    private static bool TryVectorizeUnordered(ReadOnlySpan<byte> json, Span<short> destination)
    {
        if (!TryFindObject(json, "\"transaction\""u8, out ReadOnlySpan<byte> transaction) ||
            !TryFindObject(json, "\"customer\""u8, out ReadOnlySpan<byte> customer) ||
            !TryFindObject(json, "\"merchant\""u8, out ReadOnlySpan<byte> merchant) ||
            !TryFindObject(json, "\"terminal\""u8, out ReadOnlySpan<byte> terminal) ||
            !TryFindValue(json, "\"last_transaction\""u8, out int lastValue))
        {
            return false;
        }

        Span<ulong> knownHashes = stackalloc ulong[64];
        if (!TryReadDouble(transaction, "\"amount\""u8, out double amount) ||
            !TryReadInt(transaction, "\"installments\""u8, out int installments) ||
            !TryReadString(transaction, "\"requested_at\""u8, out ReadOnlySpan<byte> requestedAt) ||
            !TryReadDouble(customer, "\"avg_amount\""u8, out double customerAvgAmount) ||
            !TryReadInt(customer, "\"tx_count_24h\""u8, out int txCount24h) ||
            !TryReadKnownMerchants(customer, knownHashes, out int knownCount) ||
            !TryReadString(merchant, "\"id\""u8, out ReadOnlySpan<byte> merchantId) ||
            !TryReadString(merchant, "\"mcc\""u8, out ReadOnlySpan<byte> mcc) ||
            !TryReadDouble(merchant, "\"avg_amount\""u8, out double merchantAvgAmount) ||
            !TryReadBool(terminal, "\"is_online\""u8, out bool isOnline) ||
            !TryReadBool(terminal, "\"card_present\""u8, out bool cardPresent) ||
            !TryReadDouble(terminal, "\"km_from_home\""u8, out double kmFromHome))
        {
            return false;
        }

        destination[0] = VectorSpec.QuantizeFlat(amount / 10_000.0);
        destination[1] = VectorSpec.QuantizeFlat(installments / 12.0);

        ParsedDateTime requestedDate = ParseDateTime(requestedAt);
        long requestedMinute = requestedDate.EpochMinute;
        destination[3] = VectorSpec.QuantizeFlat(requestedDate.Hour / 23.0);
        destination[4] = VectorSpec.QuantizeFlat(requestedDate.DayOfWeekMondayZero / 6.0);

        destination[7] = VectorSpec.QuantizeFlat(kmFromHome / 1_000.0);
        destination[8] = VectorSpec.QuantizeFlat(txCount24h / 20.0);
        destination[9] = isOnline ? (short)VectorSpec.FlatScale : (short)0;
        destination[10] = cardPresent ? (short)VectorSpec.FlatScale : (short)0;
        destination[12] = VectorSpec.QuantizeFlat(MccRisk(ParseMcc(mcc)));
        destination[13] = VectorSpec.QuantizeFlat(merchantAvgAmount / 10_000.0);

        if (lastValue < json.Length && json[lastValue] == (byte)'n')
        {
            destination[5] = (short)-VectorSpec.FlatScale;
            destination[6] = (short)-VectorSpec.FlatScale;
        }
        else
        {
            if (!TrySliceObjectAt(json, lastValue, out ReadOnlySpan<byte> lastTransaction) ||
                !TryReadString(lastTransaction, "\"timestamp\""u8, out ReadOnlySpan<byte> lastTimestamp) ||
                !TryReadDouble(lastTransaction, "\"km_from_current\""u8, out double lastKm))
            {
                return false;
            }

            long lastMinute = ParseDateTime(lastTimestamp).EpochMinute;
            destination[5] = VectorSpec.QuantizeFlat(Math.Max(0, requestedMinute - lastMinute) / 1_440.0);
            destination[6] = VectorSpec.QuantizeFlat(lastKm / 1_000.0);
        }

        FinishVector(destination, amount, customerAvgAmount, Hash(merchantId), knownHashes[..knownCount]);
        return true;
    }

    private static bool TryReadDouble(ReadOnlySpan<byte> json, ReadOnlySpan<byte> name, ref int cursor, out double value)
    {
        value = 0;
        if (!TryFindValue(json, name, ref cursor, out int valueStart))
            return false;

        if (!Utf8Parser.TryParse(json[valueStart..], out value, out int consumed) || consumed <= 0)
            return false;

        cursor = valueStart + consumed;
        return true;
    }

    private static bool TryReadInt(ReadOnlySpan<byte> json, ReadOnlySpan<byte> name, ref int cursor, out int value)
    {
        value = 0;
        if (!TryFindValue(json, name, ref cursor, out int valueStart))
            return false;

        if (!Utf8Parser.TryParse(json[valueStart..], out value, out int consumed) || consumed <= 0)
            return false;

        cursor = valueStart + consumed;
        return true;
    }

    private static bool TryReadBool(ReadOnlySpan<byte> json, ReadOnlySpan<byte> name, ref int cursor, out bool value)
    {
        value = false;
        if (!TryFindValue(json, name, ref cursor, out int valueStart))
            return false;

        if (json[valueStart..].StartsWith("true"u8))
        {
            value = true;
            cursor = valueStart + 4;
            return true;
        }

        if (json[valueStart..].StartsWith("false"u8))
        {
            cursor = valueStart + 5;
            return true;
        }

        return false;
    }

    private static bool TryReadString(ReadOnlySpan<byte> json, ReadOnlySpan<byte> name, ref int cursor, out ReadOnlySpan<byte> value)
    {
        value = default;
        if (!TryFindValue(json, name, ref cursor, out int valueStart))
            return false;

        if ((uint)valueStart >= (uint)json.Length || json[valueStart] != (byte)'"')
            return false;

        int contentStart = valueStart + 1;
        int relativeEnd = json[contentStart..].IndexOf((byte)'"');
        if (relativeEnd < 0)
            return false;

        value = json.Slice(contentStart, relativeEnd);
        cursor = contentStart + relativeEnd + 1;
        return true;
    }

    private static bool TryReadKnownMerchants(ReadOnlySpan<byte> json, ref int cursor, Span<ulong> hashes, out int count)
    {
        count = 0;
        if (!TryFindValue(json, "\"known_merchants\""u8, ref cursor, out int valueStart))
            return false;

        if ((uint)valueStart >= (uint)json.Length || json[valueStart] != (byte)'[')
            return false;

        int end = json[valueStart..].IndexOf((byte)']');
        if (end < 0)
            return false;

        int i = valueStart + 1;
        int arrayEnd = valueStart + end;
        while (i < arrayEnd)
        {
            while (i < arrayEnd && json[i] != (byte)'"')
                i++;
            if (i >= arrayEnd)
                break;

            int contentStart = i + 1;
            int relativeEnd = json[contentStart..arrayEnd].IndexOf((byte)'"');
            if (relativeEnd < 0)
                return false;

            if (count < hashes.Length)
                hashes[count++] = Hash(json.Slice(contentStart, relativeEnd));
            i = contentStart + relativeEnd + 1;
        }

        cursor = arrayEnd + 1;
        return true;
    }

    private static bool TryFindValue(ReadOnlySpan<byte> json, ReadOnlySpan<byte> name, ref int cursor, out int valueStart)
    {
        valueStart = 0;
        if ((uint)cursor >= (uint)json.Length)
            return false;

        int relativeName = json[cursor..].IndexOf(name);
        if (relativeName < 0)
            return false;

        int afterName = cursor + relativeName + name.Length;
        int relativeColon = json[afterName..].IndexOf((byte)':');
        if (relativeColon < 0)
            return false;

        valueStart = afterName + relativeColon + 1;
        while (valueStart < json.Length && IsJsonWhitespace(json[valueStart]))
            valueStart++;

        cursor = valueStart;
        return valueStart < json.Length;
    }

    private static bool TryFindValue(ReadOnlySpan<byte> json, ReadOnlySpan<byte> name, out int valueStart)
    {
        valueStart = 0;
        int relativeName = json.IndexOf(name);
        if (relativeName < 0)
            return false;

        int afterName = relativeName + name.Length;
        int relativeColon = json[afterName..].IndexOf((byte)':');
        if (relativeColon < 0)
            return false;

        valueStart = afterName + relativeColon + 1;
        while (valueStart < json.Length && IsJsonWhitespace(json[valueStart]))
            valueStart++;

        return valueStart < json.Length;
    }

    private static bool TryFindObject(ReadOnlySpan<byte> json, ReadOnlySpan<byte> name, out ReadOnlySpan<byte> value)
    {
        value = default;
        return TryFindValue(json, name, out int valueStart) && TrySliceObjectAt(json, valueStart, out value);
    }

    private static bool TrySliceObjectAt(ReadOnlySpan<byte> json, int valueStart, out ReadOnlySpan<byte> value)
    {
        value = default;
        if ((uint)valueStart >= (uint)json.Length || json[valueStart] != (byte)'{')
            return false;

        bool inString = false;
        bool escaped = false;
        int depth = 0;
        for (int i = valueStart; i < json.Length; i++)
        {
            byte current = json[i];
            if (inString)
            {
                if (escaped)
                {
                    escaped = false;
                }
                else if (current == (byte)'\\')
                {
                    escaped = true;
                }
                else if (current == (byte)'"')
                {
                    inString = false;
                }
                continue;
            }

            if (current == (byte)'"')
            {
                inString = true;
                continue;
            }

            if (current == (byte)'{')
            {
                depth++;
            }
            else if (current == (byte)'}')
            {
                depth--;
                if (depth == 0)
                {
                    value = json.Slice(valueStart, i - valueStart + 1);
                    return true;
                }
            }
        }

        return false;
    }

    private static bool TryReadDouble(ReadOnlySpan<byte> json, ReadOnlySpan<byte> name, out double value)
    {
        value = 0;
        return TryFindValue(json, name, out int valueStart) &&
            Utf8Parser.TryParse(json[valueStart..], out value, out int consumed) &&
            consumed > 0;
    }

    private static bool TryReadInt(ReadOnlySpan<byte> json, ReadOnlySpan<byte> name, out int value)
    {
        value = 0;
        return TryFindValue(json, name, out int valueStart) &&
            Utf8Parser.TryParse(json[valueStart..], out value, out int consumed) &&
            consumed > 0;
    }

    private static bool TryReadBool(ReadOnlySpan<byte> json, ReadOnlySpan<byte> name, out bool value)
    {
        value = false;
        if (!TryFindValue(json, name, out int valueStart))
            return false;

        if (json[valueStart..].StartsWith("true"u8))
        {
            value = true;
            return true;
        }

        return json[valueStart..].StartsWith("false"u8);
    }

    private static bool TryReadString(ReadOnlySpan<byte> json, ReadOnlySpan<byte> name, out ReadOnlySpan<byte> value)
    {
        value = default;
        if (!TryFindValue(json, name, out int valueStart) ||
            (uint)valueStart >= (uint)json.Length ||
            json[valueStart] != (byte)'"')
        {
            return false;
        }

        int contentStart = valueStart + 1;
        if (!TryFindStringEnd(json, contentStart, out int end))
            return false;

        value = json[contentStart..end];
        return true;
    }

    private static bool TryReadKnownMerchants(ReadOnlySpan<byte> json, Span<ulong> hashes, out int count)
    {
        count = 0;
        if (!TryFindValue(json, "\"known_merchants\""u8, out int valueStart) ||
            (uint)valueStart >= (uint)json.Length ||
            json[valueStart] != (byte)'[')
        {
            return false;
        }

        int relativeEnd = json[valueStart..].IndexOf((byte)']');
        if (relativeEnd < 0)
            return false;

        int i = valueStart + 1;
        int arrayEnd = valueStart + relativeEnd;
        while (i < arrayEnd)
        {
            while (i < arrayEnd && json[i] != (byte)'"')
                i++;
            if (i >= arrayEnd)
                break;

            int contentStart = i + 1;
            if (!TryFindStringEnd(json[..arrayEnd], contentStart, out int end))
                return false;

            if (count < hashes.Length)
                hashes[count++] = Hash(json[contentStart..end]);
            i = end + 1;
        }

        return true;
    }

    private static bool TryFindStringEnd(ReadOnlySpan<byte> json, int contentStart, out int end)
    {
        bool escaped = false;
        for (int i = contentStart; i < json.Length; i++)
        {
            byte current = json[i];
            if (escaped)
            {
                escaped = false;
                continue;
            }

            if (current == (byte)'\\')
            {
                escaped = true;
                continue;
            }

            if (current == (byte)'"')
            {
                end = i;
                return true;
            }
        }

        end = 0;
        return false;
    }

    private static void FinishVector(
        Span<short> destination,
        double amount,
        double customerAvgAmount,
        ulong merchantHash,
        ReadOnlySpan<ulong> knownHashes)
    {
        destination[2] = customerAvgAmount > 0
            ? VectorSpec.QuantizeFlat((amount / customerAvgAmount) / 10.0)
            : (short)VectorSpec.FlatScale;

        bool knownMerchant = false;
        for (int i = 0; i < knownHashes.Length; i++)
        {
            if (knownHashes[i] == merchantHash)
            {
                knownMerchant = true;
                break;
            }
        }

        destination[11] = knownMerchant ? (short)0 : (short)VectorSpec.FlatScale;
    }

    private static bool IsJsonWhitespace(byte value)
    {
        return value is (byte)' ' or (byte)'\n' or (byte)'\r' or (byte)'\t';
    }

    private static int ParseMcc(ReadOnlySpan<byte> mcc)
    {
        return mcc.Length == 4
            ? ((mcc[0] - '0') * 1000) + ((mcc[1] - '0') * 100) + ((mcc[2] - '0') * 10) + (mcc[3] - '0')
            : 0;
    }

    private static double MccRisk(int mcc)
    {
        return mcc switch
        {
            5411 => 0.15,
            5812 => 0.30,
            5912 => 0.20,
            5944 => 0.45,
            7801 => 0.80,
            7802 => 0.75,
            7995 => 0.85,
            4511 => 0.35,
            5311 => 0.25,
            5999 => 0.50,
            _ => 0.50,
        };
    }

    private static ulong Hash(ReadOnlySpan<byte> value)
    {
        ulong hash = 14_695_981_039_346_656_037UL;
        for (int i = 0; i < value.Length; i++)
        {
            hash ^= value[i];
            hash *= 1_099_511_628_211UL;
        }
        return hash;
    }

    private static int Parse2(ReadOnlySpan<byte> s, int offset) => ((s[offset] - '0') * 10) + (s[offset + 1] - '0');
    private static int Parse4(ReadOnlySpan<byte> s, int offset) => (Parse2(s, offset) * 100) + Parse2(s, offset + 2);

    private readonly record struct ParsedDateTime(long EpochMinute, int Hour, int DayOfWeekMondayZero);

    private static ParsedDateTime ParseDateTime(ReadOnlySpan<byte> iso)
    {
        int y = Parse4(iso, 0);
        int m = Parse2(iso, 5);
        int d = Parse2(iso, 8);
        int hh = Parse2(iso, 11);
        int mm = Parse2(iso, 14);
        long days = DaysFromCivil(y, m, d);
        return new ParsedDateTime(days * 1_440L + hh * 60L + mm, hh, (int)((days + 3) % 7));
    }

    private static long DaysFromCivil(int y, int m, int d)
    {
        y -= m <= 2 ? 1 : 0;
        int era = (y >= 0 ? y : y - 399) / 400;
        uint yoe = (uint)(y - era * 400);
        uint doy = (uint)((153 * (m + (m > 2 ? -3 : 9)) + 2) / 5 + d - 1);
        uint doe = yoe * 365 + yoe / 4 - yoe / 100 + doy;
        return era * 146_097L + doe - 719_468;
    }
}
