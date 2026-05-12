using System.Buffers.Text;

namespace Rinha.Core;

public static class FraudVectorizer
{
    public static bool TryVectorize(ReadOnlySpan<byte> json, Span<short> destination)
    {
        destination.Clear();

        int cursor = 0;
        Span<ulong> knownHashes = stackalloc ulong[64];
        int knownCount = 0;

        try
        {
            if (!TryReadDouble(json, "\"amount\""u8, ref cursor, out double amount))
                return false;
            destination[0] = VectorSpec.QuantizeFlat(amount / 10_000.0);

            if (!TryReadInt(json, "\"installments\""u8, ref cursor, out int installments))
                return false;
            destination[1] = VectorSpec.QuantizeFlat(installments / 12.0);

            if (!TryReadString(json, "\"requested_at\""u8, ref cursor, out ReadOnlySpan<byte> requestedAt))
                return false;
            long requestedMinute = ParseEpochMinute(requestedAt);
            destination[3] = VectorSpec.QuantizeFlat(Parse2(requestedAt, 11) / 23.0);
            destination[4] = VectorSpec.QuantizeFlat(DayOfWeekMondayZero(requestedAt) / 6.0);

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
            destination[12] = VectorSpec.QuantizeFlat(MccRisk(mcc));

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

                long lastMinute = ParseEpochMinute(lastTimestamp);
                destination[5] = VectorSpec.QuantizeFlat(Math.Max(0, requestedMinute - lastMinute) / 1_440.0);
                destination[6] = VectorSpec.QuantizeFlat(lastKm / 1_000.0);
            }

            destination[2] = customerAvgAmount > 0
                ? VectorSpec.QuantizeFlat((amount / customerAvgAmount) / 10.0)
                : (short)VectorSpec.FlatScale;

            bool knownMerchant = false;
            for (int i = 0; i < knownCount; i++)
            {
                if (knownHashes[i] == merchantHash)
                {
                    knownMerchant = true;
                    break;
                }
            }

            destination[11] = knownMerchant ? (short)0 : (short)VectorSpec.FlatScale;
            return true;
        }
        catch
        {
            destination.Clear();
            return false;
        }
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

    private static bool IsJsonWhitespace(byte value)
    {
        return value is (byte)' ' or (byte)'\n' or (byte)'\r' or (byte)'\t';
    }

    private static double MccRisk(ReadOnlySpan<byte> mcc)
    {
        if (mcc.SequenceEqual("5411"u8)) return 0.15;
        if (mcc.SequenceEqual("5812"u8)) return 0.30;
        if (mcc.SequenceEqual("5912"u8)) return 0.20;
        if (mcc.SequenceEqual("5944"u8)) return 0.45;
        if (mcc.SequenceEqual("7801"u8)) return 0.80;
        if (mcc.SequenceEqual("7802"u8)) return 0.75;
        if (mcc.SequenceEqual("7995"u8)) return 0.85;
        if (mcc.SequenceEqual("4511"u8)) return 0.35;
        if (mcc.SequenceEqual("5311"u8)) return 0.25;
        if (mcc.SequenceEqual("5999"u8)) return 0.50;
        return 0.50;
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

    private static long ParseEpochMinute(ReadOnlySpan<byte> iso)
    {
        int y = Parse4(iso, 0);
        int m = Parse2(iso, 5);
        int d = Parse2(iso, 8);
        int hh = Parse2(iso, 11);
        int mm = Parse2(iso, 14);
        return DaysFromCivil(y, m, d) * 1_440L + hh * 60L + mm;
    }

    private static int DayOfWeekMondayZero(ReadOnlySpan<byte> iso)
    {
        int y = Parse4(iso, 0);
        int m = Parse2(iso, 5);
        int d = Parse2(iso, 8);
        long days = DaysFromCivil(y, m, d);
        return (int)((days + 3) % 7);
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
