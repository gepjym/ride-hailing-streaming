-- ====================================================================================
-- SCRIPT KHỞI TẠO CSDL CHO LUẬN VĂN RIDE-HAILING
-- Dựa trên Bảng 35 của báo cáo và file DatabaseScript.txt
-- ====================================================================================

-- Bước 1: Kích hoạt các extension cần thiết
CREATE EXTENSION IF NOT EXISTS "uuid-ossp";
CREATE EXTENSION IF NOT EXISTS postgis;

---
-- Bước 2: Tạo các kiểu dữ liệu tùy chỉnh (ENUMs)
---
CREATE TYPE public."passenger_address_addresstype_enum" AS ENUM ('HOME', 'WORK', 'OTHER');

---
-- Bước 3: Tạo các bảng (tables) và chỉ mục (indexes)
---

-------------------------------------
-- Bảng: public.account
-------------------------------------
CREATE TABLE public.account (
    id uuid NOT NULL,
    "driverId" uuid NOT NULL,
    "accountType" text NULL,
    balance numeric(10, 2) DEFAULT '0'::numeric NOT NULL,
    currency text DEFAULT 'VND'::text NULL,
    "lastWithdrawalDate" timestamptz NULL,
    "createdAt" timestamptz DEFAULT now() NOT NULL,
    "updatedAt" timestamptz DEFAULT now() NOT NULL,
    "deletedAt" timestamptz NULL,
    "isVerifiedQuickWithdraw" bool DEFAULT false NOT NULL,
    CONSTRAINT "PK_54115ee388cdb6d86bb4bf5b2ea" PRIMARY KEY (id)
);
ALTER TABLE public.account REPLICA IDENTITY FULL;

-------------------------------------
-- Bảng: public.account_history
-------------------------------------
CREATE TABLE public.account_history (
    id uuid NOT NULL,
    "accountId" uuid NOT NULL,
    "transactionId" uuid NOT NULL,
    amount numeric(10, 2) NOT NULL,
    "previousBalance" numeric(10, 2) NOT NULL,
    "newBalance" numeric(10, 2) NOT NULL,
    "createdAt" timestamptz DEFAULT now() NOT NULL,
    "updatedAt" timestamptz DEFAULT now() NOT NULL,
    "deletedAt" timestamptz NULL,
    CONSTRAINT "PK_de0652296aa9d641c6269104b98" PRIMARY KEY (id)
);
ALTER TABLE public.account_history REPLICA IDENTITY FULL;

-------------------------------------
-- Bảng: public.account_transaction
-------------------------------------
CREATE TABLE public.account_transaction (
    id uuid NOT NULL,
    "driverId" uuid NOT NULL,
    "accountId" uuid NOT NULL,
    "accountType" text NULL,
    "transactionType" text NOT NULL,
    "transactionMethod" text NOT NULL,
    "transactionRef" text NOT NULL,
    status text NOT NULL,
    amount numeric(10, 2) DEFAULT '0'::numeric NOT NULL,
    currency text DEFAULT 'VND'::text NULL,
    "createdAt" timestamptz DEFAULT now() NOT NULL,
    "updatedAt" timestamptz DEFAULT now() NOT NULL,
    "deletedAt" timestamptz NULL,
    "bookingCode" text NULL,
    title text NULL,
    "requestId" uuid NULL,
    "isQuickWithdraw" bool DEFAULT false NOT NULL,
    CONSTRAINT "PK_eba337658ffe8785716a99dcb92" PRIMARY KEY (id)
);
CREATE INDEX "IDX_3723c7b61f5e8b87d676146b34" ON public.account_transaction USING btree ("driverId", "accountId", "transactionRef");
ALTER TABLE public.account_transaction REPLICA IDENTITY FULL;

-------------------------------------
-- Bảng: public.driver
-------------------------------------
CREATE TABLE public.driver (
    id uuid NOT NULL,
    "fullName" text NULL,
    email text NULL,
    "phoneNumber" int4 NOT NULL,
    "countryCode" text NOT NULL,
    status text NULL,
    "password" text NOT NULL,
    "areaCode" text NULL,
    "referralCode" text NULL,
    referrer text NULL,
    "serviceCode" text NULL,
    "currentStepRegistration" text NULL,
    "verificationStatus" text NULL,
    "imageUrl" text NULL,
    "isMoovTek" bool DEFAULT false NOT NULL,
    "createdAt" timestamptz DEFAULT now() NOT NULL,
    "updatedAt" timestamptz DEFAULT now() NOT NULL,
    "deletedAt" timestamptz NULL,
    address text NULL,
    "isAutoAssign" bool DEFAULT false NOT NULL,
    "isCore" bool DEFAULT false NOT NULL,
    "contractUrl" text NULL,
    seat int4 NULL,
    "isMoovTekPlatform" bool DEFAULT false NOT NULL,
    "oldDriverId" uuid NULL,
    CONSTRAINT "PK_61de71a8d217d585ecd5ee3d065" PRIMARY KEY (id)
);
ALTER TABLE public.driver REPLICA IDENTITY FULL;

-------------------------------------
-- Bảng: public.driver_availability_logs
-------------------------------------
CREATE TABLE public.driver_availability_logs (
    id uuid NOT NULL,
    "driverId" uuid NOT NULL,
    availability text NOT NULL,
    "createdAt" timestamptz DEFAULT now() NOT NULL,
    "updatedAt" timestamptz DEFAULT now() NOT NULL,
    "deletedAt" timestamptz NULL,
    CONSTRAINT "PK_dd2a7649a3e4e7279c61c3d004b" PRIMARY KEY (id)
);
CREATE INDEX idx_driver_availability_logs ON public.driver_availability_logs USING btree ("driverId", availability);
ALTER TABLE public.driver_availability_logs REPLICA IDENTITY FULL;

-------------------------------------
-- Bảng: public.driver_earnings (trong báo cáo là driver_earning)
-------------------------------------
CREATE TABLE public.driver_earnings (
    id uuid DEFAULT uuid_generate_v4() NOT NULL,
    "rideId" uuid NOT NULL,
    "bookingId" uuid NULL,
    "driverId" uuid NULL,
    "serviceFeeBeforeTax" int8 NOT NULL,
    "personalIncomeTax" int8 NOT NULL,
    "commissionFee" int8 NOT NULL,
    "taxAmount" int8 NOT NULL,
    "otherFee" int8 DEFAULT '0'::bigint NOT NULL,
    "driverEarning" int8 DEFAULT '0'::bigint NOT NULL,
    "totalEarnings" int8 DEFAULT '0'::bigint NOT NULL,
    "createdAt" timestamptz DEFAULT now() NOT NULL,
    "updatedAt" timestamptz DEFAULT now() NOT NULL,
    "deletedAt" timestamptz NULL,
    "bonusMoney" int8 DEFAULT '0'::bigint NOT NULL,
    "bonusPITax" int8 DEFAULT '0'::bigint NOT NULL,
    "bonusAfterPITax" int8 DEFAULT '0'::bigint NOT NULL,
    "bonusRate" numeric DEFAULT '0'::numeric NOT NULL,
    "diamondReceived" int8 DEFAULT '0'::bigint NOT NULL,
    "commissionRate" numeric DEFAULT 0.14 NOT NULL,
    "totalAmountBooking" int8 DEFAULT '0'::bigint NOT NULL,
    CONSTRAINT "PK_8e1fd49cf2a697c7e5bcc621461" PRIMARY KEY (id)
);
ALTER TABLE public.driver_earnings REPLICA IDENTITY FULL;

-------------------------------------
-- Bảng: public.daily_driver_earnings
-------------------------------------
CREATE TABLE public.daily_driver_earnings (
    id uuid DEFAULT uuid_generate_v4() NOT NULL,
    "driverId" uuid NOT NULL,
    "earningDate" date NOT NULL,
    "totalEarnings" numeric(10, 2) DEFAULT '0'::numeric NOT NULL,
    "completedTripsCount" int4 DEFAULT 0 NOT NULL,
    "createdAt" timestamptz DEFAULT now() NOT NULL,
    "updatedAt" timestamptz DEFAULT now() NOT NULL,
    "deletedAt" timestamptz NULL,
    CONSTRAINT "PK_b2c382d932605698e577fcf88d8" PRIMARY KEY (id),
    CONSTRAINT driver_date UNIQUE ("driverId", "earningDate")
);
ALTER TABLE public.daily_driver_earnings REPLICA IDENTITY FULL;

-------------------------------------
-- Bảng: public.driver_trip (trong báo cáo là driver_trips)
-------------------------------------
CREATE TABLE public.driver_trip (
    id uuid NOT NULL,
    "driverId" uuid NOT NULL,
    "rideId" uuid NOT NULL,
    "bookingId" uuid NOT NULL,
    status text NOT NULL,
    "createdAt" timestamptz DEFAULT now() NOT NULL,
    "updatedAt" timestamptz DEFAULT now() NOT NULL,
    "assignedLocation" public.geography(point, 4326) NULL,
    "assignedTripAt" timestamptz NULL,
    "acceptedLocation" public.geography(point, 4326) NULL,
    "acceptedTripAt" timestamptz NULL,
    "includeInMetric" bool DEFAULT true NOT NULL,
    CONSTRAINT "PK_c38196a9b27e2777497f8631f24" PRIMARY KEY (id),
    CONSTRAINT unique_trip_booking_driver UNIQUE ("rideId", "bookingId", "driverId")
);
CREATE INDEX "IDX_109a4107190963e49a29ef5267" ON public.driver_trip USING gist ("acceptedLocation");
CREATE INDEX "IDX_5d9392deac842f1ce19cf6fa7b" ON public.driver_trip USING gist ("assignedLocation");
CREATE INDEX idx_trip_booking_driver ON public.driver_trip USING btree ("rideId", "bookingId", "driverId");
ALTER TABLE public.driver_trip REPLICA IDENTITY FULL;

-------------------------------------
-- Bảng: public.driver_document
-------------------------------------
CREATE TABLE public.driver_document (
    id uuid NOT NULL,
    "driverId" uuid NOT NULL,
    "documentType" text NOT NULL,
    status text NOT NULL,
    "additionalInfo" json NULL,
    reason text NULL,
    "createdAt" timestamptz DEFAULT now() NOT NULL,
    "updatedAt" timestamptz DEFAULT now() NOT NULL,
    "deletedAt" timestamptz NULL,
    CONSTRAINT "PK_21c7cd21e8a72b1209b9c4bc39d" PRIMARY KEY (id)
);
CREATE INDEX idx_driver_document ON public.driver_document USING btree ("driverId", "documentType");
ALTER TABLE public.driver_document REPLICA IDENTITY FULL;

-------------------------------------
-- Bảng: public.driver_location
-------------------------------------
DROP TABLE IF EXISTS public.driver_location CASCADE;
CREATE TABLE public.driver_location (
  id           UUID PRIMARY KEY,
  "driverId"   UUID NOT NULL UNIQUE,
  coordinates  geography(Point, 4326),
  longitude    DOUBLE PRECISION GENERATED ALWAYS AS (ST_X(coordinates::geometry)) STORED,
  latitude     DOUBLE PRECISION GENERATED ALWAYS AS (ST_Y(coordinates::geometry)) STORED,
  availability TEXT NOT NULL DEFAULT 'UNAVAILABLE',
  "createdAt"  TIMESTAMPTZ NOT NULL DEFAULT now(),
  "updatedAt"  TIMESTAMPTZ NOT NULL DEFAULT now(),
  "deletedAt"  TIMESTAMPTZ
);
-- Chỉ mục
CREATE INDEX IF NOT EXISTS idx_driver_location_gist  ON public.driver_location USING gist (coordinates);
CREATE INDEX IF NOT EXISTS idx_driver_location_btree ON public.driver_location ("driverId", availability);
-- Đảm bảo mỗi tài xế chỉ có một bản ghi vị trí hiện thời
CREATE UNIQUE INDEX IF NOT EXISTS ux_driver_location_driver ON public.driver_location ("driverId");

-- Cập nhật "updatedAt" tự động khi UPDATE
CREATE OR REPLACE FUNCTION set_updated_at_driver_location()
RETURNS trigger LANGUAGE plpgsql AS $$
BEGIN
  NEW."updatedAt" := now();
  RETURN NEW;
END$$;
DROP TRIGGER IF EXISTS trg_set_updated_at_driver_location ON public.driver_location;
CREATE TRIGGER trg_set_updated_at_driver_location
BEFORE UPDATE ON public.driver_location
FOR EACH ROW EXECUTE FUNCTION set_updated_at_driver_location();

-- Để Debezium gửi đầy đủ "after" khi UPDATE/DELETE
ALTER TABLE public.driver_location REPLICA IDENTITY FULL;

-------------------------------------
-- Bảng: public.passenger
-------------------------------------
CREATE TABLE public.passenger (
    id uuid NOT NULL,
    "fullName" text NULL,
    email text NULL,
    "phoneNumber" int8 NOT NULL,
    "countryCode" text NOT NULL,
    status text NULL,
    "password" text NOT NULL,
    "urlAvatar" text NULL,
    birthday timestamptz NULL,
    "referralCode" text NULL,
    referrer text NULL,
    "isMoovTek" bool DEFAULT false NOT NULL,
    "createdAt" timestamptz DEFAULT now() NOT NULL,
    "updatedAt" timestamptz DEFAULT now() NOT NULL,
    "deletedAt" timestamptz NULL,
    "isPublicTest" bool DEFAULT false NOT NULL,
    CONSTRAINT "PK_50e940dd2c126adc20205e83fac" PRIMARY KEY (id)
);
ALTER TABLE public.passenger REPLICA IDENTITY FULL;

-------------------------------------
-- Bảng: public.passenger_address
-------------------------------------
CREATE TABLE public.passenger_address (
    id uuid NOT NULL,
    "passengerId" text NOT NULL,
    "name" text NOT NULL,
    address text NOT NULL,
    "addressType" public."passenger_address_addresstype_enum" DEFAULT 'OTHER'::passenger_address_addresstype_enum NOT NULL,
    coordinates public.geography(point, 4326) NULL,
    "createdAt" timestamptz DEFAULT now() NOT NULL,
    "updatedAt" timestamptz DEFAULT now() NOT NULL,
    "deletedAt" timestamptz NULL,
    CONSTRAINT "PK_8529831dd749d937b20833c6bee" PRIMARY KEY (id)
);
CREATE INDEX "IDX_aae9d9cd7a3cbe4ba29efa2434" ON public.passenger_address USING gist (coordinates);
ALTER TABLE public.passenger_address REPLICA IDENTITY FULL;

-------------------------------------
-- Bảng: public.payment_methods
-------------------------------------
CREATE TABLE public.payment_methods (
    id uuid NOT NULL,
    "passengerId" text NOT NULL,
    partner text NOT NULL,
    "paymentToken" varchar(255) NOT NULL,
    "bankCode" varchar(50) NOT NULL,
    "cardType" varchar(100) NOT NULL,
    "maskedCardNumber" varchar(25) NULL,
    "isDefault" bool DEFAULT false NOT NULL,
    "createdAt" timestamptz DEFAULT now() NOT NULL,
    "updatedAt" timestamptz DEFAULT now() NOT NULL,
    "deletedAt" timestamptz NULL,
    "icIssue" text NULL,
    "accountLinkId" varchar(40) NULL,
    "bankName" text NULL,
    "bankId" varchar(50) NULL,
    "isBusiness" bool DEFAULT false NOT NULL,
    CONSTRAINT "PK_34f9b8c6dfb4ac3559f7e2820d1" PRIMARY KEY (id)
);
ALTER TABLE public.payment_methods REPLICA IDENTITY FULL;

-------------------------------------
-- Bảng: public.payment_transactions
-------------------------------------
CREATE TABLE public.payment_transactions (
    id uuid NOT NULL,
    "passengerId" uuid NULL,
    "paymentMethodId" uuid NULL,
    "transRef" varchar(100) NOT NULL,
    amount numeric(18, 2) NOT NULL,
    currency varchar(3) NOT NULL,
    "partnerResponse" json NULL,
    status varchar(20) NOT NULL,
    "createdAt" timestamptz DEFAULT now() NOT NULL,
    "updatedAt" timestamptz DEFAULT now() NOT NULL,
    "deletedAt" timestamptz NULL,
    "driverId" uuid NULL,
    "paymentTransactionType" varchar NULL,
    "bookingId" uuid NULL,
    "bookingCode" text NULL,
    fee numeric(18, 2) NULL,
    CONSTRAINT "PK_d32b3c6b0d2c1d22604cbcc8c49" PRIMARY KEY (id)
);
ALTER TABLE public.payment_transactions REPLICA IDENTITY FULL;

-------------------------------------
-- Bảng: public.rides
-------------------------------------
CREATE TABLE public.rides (
    id uuid DEFAULT uuid_generate_v4() NOT NULL,
    "driverId" uuid NULL,
    "startAddress" text NULL,
    "startLocation" public.geography(point, 4326) NULL,
    "endAddress" text NULL,
    "endLocation" public.geography(point, 4326) NULL,
    status text DEFAULT 'REQUESTED'::text NULL,
    seats int8 DEFAULT '1'::bigint NOT NULL,
    "serviceTypeCode" text NULL,
    "serviceTierCode" text NULL,
    "cancelAt" timestamptz NULL,
    "createdAt" timestamptz DEFAULT now() NOT NULL,
    "updatedAt" timestamptz DEFAULT now() NOT NULL,
    "deletedAt" timestamptz NULL,
    "serviceVariantsCode" text NULL,
    "isMoovTekPool" bool DEFAULT false NULL,
    "totalAmount" int8 DEFAULT '0'::bigint NULL,
    CONSTRAINT "PK_ca6f62fc1e999b139c7f28f07fd" PRIMARY KEY (id)
);
CREATE INDEX "IDX_03c9ed6480c94f7db392d708d5" ON public.rides USING gist ("endLocation");
CREATE INDEX "IDX_788299bfe26f7f0aff9ddc189e" ON public.rides USING gist ("startLocation");
CREATE INDEX idx_ride ON public.rides USING btree ("driverId", status);
ALTER TABLE public.rides REPLICA IDENTITY FULL;

-------------------------------------
-- Bảng: public.booking
-------------------------------------
CREATE TABLE public.booking (
    id uuid DEFAULT uuid_generate_v4() NOT NULL,
    "rideId" uuid NULL,
    "bookingCode" text NULL,
    "passengerId" uuid NOT NULL,
    "pickupAddress" text NULL,
    "pickupLocation" public.geography(point, 4326) NULL,
    "dropoffAddress" text NULL,
    "dropoffLocation" public.geography(point, 4326) NULL,
    "paymentMethodId" uuid NULL,
    "paymentMethodType" text NULL,
    "serviceTypeCode" text NULL,
    "serviceTierCode" text NULL,
    "discountId" uuid[] NULL, -- Corrected from _uuid
    "discountAmount" int8 DEFAULT '0'::bigint NOT NULL,
    "platformFee" int8 DEFAULT '0'::bigint NOT NULL,
    "totalAmountBeforeDiscount" int8 DEFAULT '0'::bigint NOT NULL,
    "totalAmountAfterDiscount" int8 DEFAULT '0'::bigint NOT NULL,
    "additionalCharges" int8 DEFAULT '0'::bigint NOT NULL,
    status text DEFAULT 'REQUESTED'::text NULL,
    "startTripAt" timestamptz NULL,
    "atPickUpAt" timestamptz NULL,
    "completedAt" timestamptz NULL,
    "createdAt" timestamptz DEFAULT now() NOT NULL,
    "updatedAt" timestamptz DEFAULT now() NOT NULL,
    "deletedAt" timestamptz NULL,
    "totalAmount" int8 DEFAULT '0'::bigint NOT NULL,
    "startTripLocation" public.geography(point, 4326) NULL,
    "atPickUpLocation" public.geography(point, 4326) NULL,
    "completedLocation" public.geography(point, 4326) NULL,
    "areaCode" text NULL,
    "wardCode" text NULL,
    "serviceVariantsCode" text NULL,
    seats int8 DEFAULT '1'::bigint NULL,
    "rewardId" text NULL,
    "bookingType" text DEFAULT 'MOOV'::text NULL,
    "moovNowCode" int8 DEFAULT '0'::bigint NULL,
    "passengerInvoiceId" uuid NULL,
    "ipAddress" text NULL,
    "deviceInfo" jsonb NULL,
    "orgId" uuid NULL,
    CONSTRAINT "PK_49171efc69702ed84c812f33540" PRIMARY KEY (id)
);
CREATE INDEX "IDX_753558e842f249b157c6907cb1" ON public.booking USING gist ("startTripLocation");
CREATE INDEX "IDX_7c5e1bc5469949963bfc761da1" ON public.booking USING gist ("completedLocation");
CREATE INDEX "IDX_8cd8e5a1eec89c76ca51f03fd5" ON public.booking USING gist ("atPickUpLocation");
CREATE INDEX "IDX_d301c8c32311b6e100b1e6894b" ON public.booking USING gist ("pickupLocation");
CREATE INDEX "IDX_e6eeb6e7ea0194664535a5584d" ON public.booking USING gist ("dropoffLocation");
CREATE INDEX idx_booking ON public.booking USING btree ("passengerId", "rideId", status, "serviceTypeCode");
ALTER TABLE public.booking REPLICA IDENTITY FULL;


-------------------------------------
-- Bảng: public.booking_cancellations
-------------------------------------
CREATE TABLE public.booking_cancellations (
    id uuid DEFAULT uuid_generate_v4() NOT NULL,
    "rideId" uuid NULL,
    "reasonId" uuid[] NULL, -- Corrected from _uuid
    "reasonDetail" text NULL,
    "cancelledById" uuid NOT NULL,
    "cancelledBy" text NOT NULL,
    "cancelledAt" timestamptz DEFAULT now() NOT NULL,
    "createdAt" timestamptz DEFAULT now() NOT NULL,
    "updatedAt" timestamptz DEFAULT now() NOT NULL,
    "deletedAt" timestamptz NULL,
    "bookingId" uuid NULL,
    status text DEFAULT 'PENDING'::text NULL,
    "approvedBy" uuid NULL,
    "rejectReason" text NULL,
    "evidenceId" uuid NULL,
    CONSTRAINT "PK_8bb840ff63f3a96ed3a75c4bfef" PRIMARY KEY (id)
);
ALTER TABLE public.booking_cancellations REPLICA IDENTITY FULL;

-- ====================================================================================
-- PHẦN 2: CÁC BẢNG PHỤ TRỢ (TIẾP THEO TỪ BẢNG 35)
-- ====================================================================================

-------------------------------------
-- Bảng: public.deposit_detail
-------------------------------------
CREATE TABLE public.deposit_detail (
    id uuid NOT NULL,
    "transactionId" uuid NOT NULL,
    "additionalInfo" json NOT NULL,
    "createdAt" timestamptz DEFAULT now() NOT NULL,
    "updatedAt" timestamptz DEFAULT now() NOT NULL,
    "deletedAt" timestamptz NULL,
    CONSTRAINT "PK_64598c7582571111beb34104261" PRIMARY KEY (id)
);
ALTER TABLE public.deposit_detail REPLICA IDENTITY FULL;

-------------------------------------
-- Bảng: public.with_drawal_detail (Lưu ý: tên trong DDL, báo cáo là with_drawal_detail)
-------------------------------------
CREATE TABLE public.with_drawal_detail (
    id uuid NOT NULL,
    "transactionId" uuid NOT NULL,
    "additionalInfo" json NULL,
    "createdAt" timestamptz DEFAULT now() NOT NULL,
    "updatedAt" timestamptz DEFAULT now() NOT NULL,
    "deletedAt" timestamptz NULL,
    CONSTRAINT "PK_cabab90cdc7668c3a78ccad8419" PRIMARY KEY (id)
);
ALTER TABLE public.with_drawal_detail REPLICA IDENTITY FULL;

-------------------------------------
-- Bảng: public.area
-------------------------------------
CREATE TABLE public.area (
    id uuid NOT NULL,
    "createdAt" timestamptz(6) DEFAULT now() NOT NULL,
    "updatedAt" timestamptz(6) DEFAULT now() NOT NULL,
    "deletedAt" timestamptz(6) NULL,
    "provinceCode" text NULL,
    "serviceCodes" text[] DEFAULT '{}'::text[] NOT NULL,
    CONSTRAINT "PK_39d5e4de490139d6535d75f42ff" PRIMARY KEY (id)
);
ALTER TABLE public.area REPLICA IDENTITY FULL;

-------------------------------------
-- Bảng: public.area_district
-------------------------------------
CREATE TABLE public.area_district (
    id uuid NOT NULL,
    "name" text NOT NULL,
    code text NOT NULL,
    "provinceCode" text NOT NULL,
    "createdAt" timestamptz(6) DEFAULT now() NOT NULL,
    "updatedAt" timestamptz(6) DEFAULT now() NOT NULL,
    "deletedAt" timestamptz(6) NULL,
    CONSTRAINT "PK_623caa622a7e157b50f6dbc9e72" PRIMARY KEY (id)
);
ALTER TABLE public.area_district REPLICA IDENTITY FULL;

-------------------------------------
-- Bảng: public.area_province
-------------------------------------
CREATE TABLE public.area_province (
    id uuid NOT NULL,
    "name" text NOT NULL,
    code text NOT NULL,
    "createdAt" timestamptz(6) DEFAULT now() NOT NULL,
    "updatedAt" timestamptz(6) DEFAULT now() NOT NULL,
    "deletedAt" timestamptz(6) NULL,
    boundary public.geometry(multipolygon, 4326) NULL,
    CONSTRAINT "PK_14eb73271fee482a79370c9ff42" PRIMARY KEY (id)
);
ALTER TABLE public.area_province REPLICA IDENTITY FULL;

-------------------------------------
-- Bảng: public.area_ward
-------------------------------------
CREATE TABLE public.area_ward (
    id uuid NOT NULL,
    "name" text NOT NULL,
    code text NOT NULL,
    "districtCode" text NOT NULL,
    "provinceCode" text NOT NULL,
    "createdAt" timestamptz(6) DEFAULT now() NOT NULL,
    "updatedAt" timestamptz(6) DEFAULT now() NOT NULL,
    "deletedAt" timestamptz(6) NULL,
    CONSTRAINT "PK_a37571a34e95a3b553bf7d25f88" PRIMARY KEY (id)
);
ALTER TABLE public.area_ward REPLICA IDENTITY FULL;

-------------------------------------
-- Bảng: public.driver_training_courses
-------------------------------------
CREATE TABLE public.driver_training_courses (
    id uuid NOT NULL,
    title varchar(255) NOT NULL,
    mandatory bool NOT NULL,
    "additionalInfo" json NOT NULL,
    "createdAt" timestamptz(6) DEFAULT now() NOT NULL,
    "updatedAt" timestamptz(6) DEFAULT now() NOT NULL,
    "deletedAt" timestamptz(6) NULL,
    "order" int4 DEFAULT 0 NULL,
    CONSTRAINT "PK_8fca3491c273537a4e81ac52033" PRIMARY KEY (id)
);
ALTER TABLE public.driver_training_courses REPLICA IDENTITY FULL;

-------------------------------------
-- Bảng: public.driver_training_questions
-------------------------------------
CREATE TABLE public.driver_training_questions (
    id uuid NOT NULL,
    "questionText" text NOT NULL,
    "options" varchar[] NOT NULL, -- Sửa _varchar thành varchar[]
    "createdAt" timestamptz(6) DEFAULT now() NOT NULL,
    "updatedAt" timestamptz(6) DEFAULT now() NOT NULL,
    "additionalInfo" json NOT NULL,
    "correctAnswer" int2 NOT NULL,
    "order" int4 DEFAULT 0 NULL,
    "deletedAt" timestamptz NULL,
    CONSTRAINT "PK_be34bdcc8587da7c3801ae5d893" PRIMARY KEY (id)
);
ALTER TABLE public.driver_training_questions REPLICA IDENTITY FULL;

-------------------------------------
-- Bảng: public.driver_training_record
-------------------------------------
CREATE TABLE public.driver_training_record (
    id uuid NOT NULL,
    "driverId" uuid NOT NULL,
    "courseId" uuid NOT NULL,
    "questionId" uuid NOT NULL,
    "driverAnswer" int2 NOT NULL,
    "createdAt" timestamptz(6) DEFAULT now() NOT NULL,
    "updatedAt" timestamptz(6) DEFAULT now() NOT NULL,
    CONSTRAINT "PK_870f29bebef1ffd74559d0e6307" PRIMARY KEY (id)
);
ALTER TABLE public.driver_training_record REPLICA IDENTITY FULL;

-------------------------------------
-- Bảng: public.driver_training_course_completion
-------------------------------------
CREATE TABLE public.driver_training_course_completion (
    id uuid NOT NULL,
    "driverId" uuid NOT NULL,
    "courseId" uuid NOT NULL,
    "completionStatus" text NOT NULL,
    "completionPercentage" int4 DEFAULT 0 NOT NULL,
    "createdAt" timestamptz DEFAULT now() NOT NULL,
    "updatedAt" timestamptz DEFAULT now() NOT NULL,
    CONSTRAINT "PK_b03a687f821ada2458f021b3c42" PRIMARY KEY (id)
);
ALTER TABLE public.driver_training_course_completion REPLICA IDENTITY FULL;

-------------------------------------
-- Bảng: public.course_question_bridge
-------------------------------------
CREATE TABLE public.course_question_bridge (
    id uuid NOT NULL,
    "courseId" uuid NOT NULL,
    "questionId" uuid NOT NULL,
    "createdAt" timestamptz(6) DEFAULT now() NOT NULL,
    "updatedAt" timestamptz(6) DEFAULT now() NOT NULL,
    "deletedAt" timestamptz(6) NULL,
    CONSTRAINT "PK_5dd971fec27b200ac055d82be71" PRIMARY KEY (id)
);
ALTER TABLE public.course_question_bridge REPLICA IDENTITY FULL;

-- ====================================================================================
-- KẾT THÚC SCRIPT
-- ====================================================================================